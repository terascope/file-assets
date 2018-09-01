'use strict';

const Promise = require('bluebird');
const fse = require('fs-extra');
const glob = require('glob');
const _ = require('lodash');
const chunkedReader = require('@terascope/chunked-file-reader');
const { getOpConfig } = require('@terascope/job-components');

// [offset, length][] of chunks `size` for a file `total`.
function offsets(size, total, delimiter) {
    // TODO: The chucked-file-reader repo a better home for this function?
    const fullChunks = Math.floor(total / size);
    const delta = delimiter.length;
    const length = size + delta;
    const chunks = _.range(1, fullChunks).map(chunk => (
        [(chunk * size) - delta, length]
    ));
    // First chunk doesn't need +/- delta.
    chunks.unshift([0, size]);
    // When last chunk is not full chunk size.
    const lastChunk = total % size;
    if (lastChunk > 0) {
        chunks.push([(fullChunks * size) - delta, lastChunk + delta]);
    }
    return chunks;
}

async function newSlicer(context, job, retryData, logger) {
    // Slicer queue.  Slices from multiple files can enqueue here but that's ok.
    const slices = [];

    const opConfig = getOpConfig(job.config, 'compressed_file_reader');
    const assetDir = await context.apis.assets.getPath('file-assets');

    const filedb = require('./filedb')(opConfig.workDir, assetDir);

    async function sliceFile(src, ready) {
        const total = await fse.stat(ready).size;
        // NOTE: Important to update `slices` syncronously so that the correct
        // `last` slice is marked.
        offsets(opConfig.size, total, opConfig.delimiter)
            .map(i => ({
                total, ready, src, offset: i[0], length: i[1],
            })).forEach(i => slices.push(i));
        // Mark the last slice so we know when to archive the file.
        slices[slices.length - 1].last = true;
        logger.info({ src, ready }, 'sliced');
    }

    let globbed = false;
    function onUpstreamGlobbed(err, paths) {
        if (err) {
            logger.error(err, 'glob error');
        }
        // Decompress files concurrently but slice sequentially.
        // TODO: We're still waiting for all to decompress before can begin slicing.
        Promise.resolve(paths)
            .filter(src => fse.exists(`${src}.ready`))
            .map(src => filedb.ready(src))
            .each(i => sliceFile(i[0], i[1]))
            .then(() => {
                globbed = true;
            })
            .catch(e => logger.error(e));
    }

    // Watch upstream directory.
    const globOpts = {
        strict: true,
        silent: false,
        noglobstar: true,
        realpath: true,
    };
    if (job.config.lifecycle === 'persistent') {
        setInterval(() => glob(opConfig.glob, globOpts, onUpstreamGlobbed), 10 * 1000);
    } else {
        glob(opConfig.glob, globOpts, onUpstreamGlobbed);
    }

    const events = context.foundation.getEventEmitter();

    async function onSlice(slice) {
        // slice: {
        //   slice: {
        //     "slice_id": "ffb513c3-5bff-4049-ba04-170735692dd2",
        //     "request": {
        //       "total": 439597,
        //       "src": "/tmp/upload/t1.json.lz4",
        //       "ready": "/tmp/work/ready/t1.json.lz4",
        //       "length": 39598,
        //       "offset": 399999
        //     },
        //     "slicer_id": 0,
        //     "slicer_order": 5,
        //     "_created": "2018-08-28T23:28:10.669Z"
        //   }
        // }
        const { request } = slice.slice;
        const { src } = request;
        if (request.last) {
            try {
                await filedb.archive(src);
                await fse.unlink(`${src}.ready`);
                logger.info({ src }, 'archived');
            } catch (err) {
                logger.error({ src }, 'failed to archive');
            }
        }
    }
    events.on('slice:success', onSlice);

    // TODO: There's gotta be a more elegant way to accomplish this.
    // Returning `undefined` for lifecycle=once jobs treated as slicing complete.
    function waitForFirstGlob() {
        return new Promise((resolve) => {
            function wait() {
                if (globbed) {
                    resolve();
                } else {
                    setTimeout(wait, 100);
                }
            }
            setTimeout(wait, 100);
        });
    }

    function nextSlice() {
        const slice = slices.shift();
        if (!slice && job.config.lifecycle === 'once') {
            return null;
        }
        // When no slices, will be `undefined` (aka keep going).
        return slice;
    }

    // TODO: test cases:
    // - partially readied (eg decompress fails)
    // - partially archived (eg rename fails)
    // - readied but not sliced
    // - one file fails in onUpstreamGlobbed
    // - file not decompressed before next glob cycle

    return waitForFirstGlob()
        .then(() => [nextSlice])
        .catch(logger.err);
}

function newReader(context, opConfig) {
    return function processSlice(slice, logger) {
        async function reader(offset, length) {
            const fd = await fse.open(slice.ready, 'r');
            try {
                const buf = Buffer.alloc(2 * opConfig.size);
                const { bytesRead } = await fse.read(fd, buf, 0, length, offset);
                return buf.slice(0, bytesRead).toString();
            } finally {
                fse.close(fd);
            }
        }
        // TODO: Patch chunkedReader to make `opConfig.format` optional
        return chunkedReader.getChunk(reader, slice, opConfig, logger);
    };
}

function schema() {
    return {
        glob: {
            doc: 'Glob pattern to match files to process.',
            default: '/tmp/upload/*',
            format: 'required_String',
        },
        workDir: {
            doc: 'Base directory for temp space while processing files.',
            default: '/tmp/work',
            format: 'required_String',
        },
        delimiter: {
            doc: 'Determines the delimiter used in the file being read. '
                + 'Currently only supports "\n"',
            default: '\n',
            format: String,
        },
        format: {
            doc: 'Format of the target file. Currently only supports "json"',
            default: 'json',
            format: ['json']
        },
        size: {
            doc: 'Determines slice size in bytes',
            default: 100000,
            format: Number,
        },
    };
}

module.exports = {
    newReader,
    newSlicer,
    schema,
};
