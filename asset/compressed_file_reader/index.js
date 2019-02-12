'use strict';

const Promise = require('bluebird');
const fse = require('fs-extra');
const os = require('os');
const glob = require('glob');
const _ = require('lodash');
const { getOffsets, getChunk } = require('@terascope/chunked-file-reader');
const { getOpConfig } = require('@terascope/job-components');

async function newSlicer(context, job, retryData, logger) {
    // Slicer queue.  Slices from multiple files can enqueue here but that's ok.
    const slices = [];

    // Limit concurrency, but do so in a way that one large upload will not hold
    // up the entire batch the way Promise.map({concurrency}) would.
    const working = [];

    const opConfig = getOpConfig(job.config, 'compressed_file_reader');
    const assetDir = await context.apis.assets.getPath('file-assets');

    const filedb = await require('./filedb')(opConfig.workDir, assetDir);

    async function sliceFile(src, work) {
        const stat = await fse.stat(work);
        const total = stat.size;
        // NOTE: Important to update `slices` syncronously so that the correct
        // `last` slice is marked.
        getOffsets(opConfig.size, total, opConfig.delimiter).forEach((offset) => {
            slices.push({
                total,
                work,
                src,
                ...offset,
            });
        });
        // Mark the last slice so we know when to archive the file.
        slices[slices.length - 1].last = true;
        _.pull(working, src);
        return work;
    }

    function doWork() {
        if (filedb.numReady() === 0) {
            return;
        }
        _.range(opConfig.concurrency - working.length).forEach(async () => {
            const src = filedb.enqueue();
            if (src) {
                logger.info({ src }, 'decompressing');
                working.push(src);
                try {
                    sliceFile(src, await filedb.decompress(src));
                    logger.info({ src }, 'sliced & ready to read');
                } catch (err) {
                    // Not sure how to handle. Retry with exponential backoff before giving up?
                    _.pull(working, src);
                    logger.error({ stderr: err.stderr }, err);
                }
            }
        });
    }

    function onUpstreamGlobbed(err, paths) {
        if (err) {
            // Can have false positive if file globbed same time it's being
            // archived. Only saw when testing against small files. Let's wait
            // to see how noisy before handling more quietly.
            logger.error(err, 'glob error');
            return;
        }
        paths.forEach(async (src) => {
            if (await filedb.uploaded(src)) {
                logger.info({ src }, 'uploaded');
            }
        });
    }

    setInterval(doWork, 1000);

    // Watch upstream directory.
    const globOpts = {
        strict: true,
        silent: false,
        noglobstar: true,
        realpath: true,
    };
    if (job.config.lifecycle === 'persistent') {
        setInterval(() => glob(opConfig.glob, globOpts, onUpstreamGlobbed), opConfig.globSeconds * 1000);
    }
    glob(opConfig.glob, globOpts, onUpstreamGlobbed);

    const events = context.foundation.getEventEmitter();

    async function onSlice(slice) {
        // slice: {
        //   slice: {
        //     "slice_id": "ffb513c3-5bff-4049-ba04-170735692dd2",
        //     "request": {
        //       "total": 439597,
        //       "src": "/tmp/upload/t1.json.lz4",
        //       "work": "/tmp/work/slice/t1.json.lz4",
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
                logger.info({ src, archive }, 'archived');
            } catch (err) {
                logger.error({ src }, 'failed to archive');
            }
        }
    }
    events.on('slice:success', onSlice);

    // Returning `undefined` for lifecycle=once jobs treated as slicing
    // complete, so give globbing a chance to complete.
    function waitForWorkToSettle() {
        return new Promise((resolve) => {
            if (job.config.lifecycle === 'persistent') {
                resolve();
                return;
            }
            function wait() {
                if (working.length) {
                    logger.debug({ working: working.length }, 'waiting for work to settle');
                    setTimeout(wait, 1000);
                } else {
                    resolve();
                }
            }
            setTimeout(wait, 1000);
        });
    }

    return waitForWorkToSettle()
        .then(() => [() => slices.shift()])
        .catch(logger.err);
}

function newReader(context, opConfig) {
    return function processSlice(slice, logger) {
        async function reader(offset, length) {
            const fd = await fse.open(slice.work, 'r');
            try {
                const buf = Buffer.alloc(2 * opConfig.size);
                const { bytesRead } = await fse.read(fd, buf, 0, length, offset);
                return buf.slice(0, bytesRead).toString();
            } finally {
                fse.close(fd);
            }
        }
        // Passing the slice in as the `metadata`. This will include the path, offset, and length
        return getChunk(reader, slice, opConfig, logger, slice);
    };
}

function schema() {
    return {
        glob: {
            doc: 'Glob pattern to match files to process.',
            default: '/tmp/upload/*',
            format: 'required_String',
        },
        globSeconds: {
            doc: 'Rate at which to check for new uploads.',
            default: 10,
            format: Number,
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
            doc: 'How to parse: `json` is newline-delimited json, `raw` leaves parsing for downstream operations.',
            default: 'raw',
            format: ['raw', 'json']
        },
        size: {
            doc: 'Determines slice size in bytes',
            default: 100000,
            format: Number,
        }
    };
}

module.exports = {
    newReader,
    newSlicer,
    schema,
};
