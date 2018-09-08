'use strict';

const Promise = require('bluebird');
const fse = require('fs-extra');
const os = require('os');
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
        { length, offset: (chunk * size) - delta }
    ));
    // First chunk doesn't need +/- delta.
    chunks.unshift({ offset: 0, length: size });
    // When last chunk is not full chunk size.
    const lastChunk = total % size;
    if (lastChunk > 0) {
        chunks.push({ offset: (fullChunks * size) - delta, length: lastChunk + delta });
    }
    return chunks;
}

async function newSlicer(context, job, retryData, logger) {
    // Slicer queue.  Slices from multiple files can enqueue here but that's ok.
    const slices = [];

    const opConfig = getOpConfig(job.config, 'compressed_file_reader');
    const assetDir = await context.apis.assets.getPath('file-assets');

    const filedb = await require('./filedb')(opConfig.workDir, assetDir);

    async function sliceFile(src, ready) {
        const stat = await fse.stat(ready);
        const total = stat.size;
        // NOTE: Important to update `slices` syncronously so that the correct
        // `last` slice is marked.
        offsets(opConfig.size, total, opConfig.delimiter).forEach((offset) => {
            slices.push({
                total,
                ready,
                src,
                ...offset,
            });
        });
        // Mark the last slice so we know when to archive the file.
        slices[slices.length - 1].last = true;
        _.pull(working, src);
        return ready;
    }

    // Limit concurrency, but do so in a way that one large upload will not hold
    // up the entire batch the way Promise.map({concurrency}) would. Push to a
    // waiting queue and then pop from that to a working queue of size
    // opConfig.concurrency. Once a file gets sliced, then replace it in the
    // working queue with one from working queue.
    const work = [];
    const working = [];

    // Track how long there has been work (ie globs matched) so we can know when
    // to proceed for lifecycle=once jobs.
    let tsSansWork = Date.now();
    let tsLastWork;

    function doWork() {
        if (work.length === 0) {
            if (tsLastWork) {
                tsSansWork = tsLastWork;
                tsLastWork = null;
            }
            return;
        }
        tsSansWork = null;
        tsLastWork = Date.now();
        _.range(opConfig.concurrency - working.length).forEach(async () => {
            const src = work.shift();
            if (src) {
                working.push(src);
                sliceFile(src, await filedb.ready(src));
            }
        });
    }

    function onUpstreamGlobbed(err, paths) {
        if (err) {
            logger.error(err, 'glob error');
        }
        paths.forEach(async (src) => {
            if (await fse.exists(`${src}.ready`)) {
                work.push(src);
            }
        });
    }

    setInterval(doWork, 100);

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
                const archive = await filedb.archive(src);
                await fse.unlink(`${src}.ready`);
                logger.info({ src, archive }, 'archived');
            } catch (err) {
                logger.error({ src }, 'failed to archive');
            }
        }
    }
    events.on('slice:success', onSlice);

    // Returning `undefined` for lifecycle=once jobs treated as slicing
    // complete, so give globbing a chance to complete.
    function waitForGlobbingToSettle() {
        return new Promise((resolve) => {
            if (job.config.lifecycle === 'persistent') {
                resolve();
            }
            function wait() {
                if (tsSansWork && Date.now() - tsSansWork > 5000) {
                    resolve();
                } else {
                    logger.warn({ ms: Date.now() - tsSansWork, slices: slices.length }, 'waiting for settle');
                    setTimeout(wait, 1000);
                }
            }
            setTimeout(wait, 1000);
        });
    }

    function nextSlice() {
        return slices.shift();
        // const slice = slices.shift();
        // if (!slice && job.config.lifecycle === 'once') {
        //     // TODO: This needed now that we're waiting for first glob?
        //     return null;
        // }
        // // When no slices, will be `undefined` (aka keep going).
        // return slice;
    }

    return waitForGlobbingToSettle()
        .then(() => [nextSlice])
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
        concurrency: {
            doc: 'Number of uploads to process concurrently.',
            default: os.cpus().length,
            format: Number,
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
