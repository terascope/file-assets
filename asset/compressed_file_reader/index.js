'use strict';

const Promise = require('bluebird');
const fse = require('fs-extra');
const path = require('path');
const glob = require('glob');
const _ = require('lodash');
const chunkedReader = require('@terascope/chunked-file-reader');
const { getOpConfig } = require('@terascope/job-components');
const { spawn } = require('child_process');

const parallelSlicers = false;

function newSlicer(context, job, retryData, logger) {
    const slices = [];

    const opConfig = getOpConfig(job.config, 'compressed_file_reader');

    const tmpPath = name => path.join(opConfig.workDir, 'tmp', name);
    const readyPath = name => path.join(opConfig.workDir, 'ready', name);
    const archivePath = name => path.join(opConfig.workDir, 'archive', name);

    Promise.map([tmpPath(''), readyPath(''), archivePath('')], dir => fse.mkdirs(dir))
        .catch(err => logger.error(err, 'failed to setup working dirs'));
    // await fse.mkdirs(tmpPath(''));
    // await fse.mkdirs(readyPath(''));
    // await fse.mkdirs(archivePath(''));

    // Use a file in workDir to store state.
    const filedb = require('./filedb')(path.join(opConfig.workDir, 'compressed_file_reader.json'));

    function sliceFile(src, ready) {
        // Chunks should overlap by size of delimiter due to how chunked-file-reader works.
        fse.stat(ready).then((stat) => {
            const total = stat.size;
            const numFullChunks = Math.floor(total / opConfig.size);
            const delta = opConfig.delimiter.length;
            const length = opConfig.size + delta;
            // logger.warn({ ready, total, numFullChunks }, 'TODO: DOOD?');
            _.range(1, numFullChunks).forEach((chunk) => {
                slices.push({
                    total,
                    length,
                    src,
                    ready,
                    offset: (chunk * opConfig.size) - delta,
                });
            });
            // Handle first & last chunks specially.
            slices.unshift({ total, src, ready, offset: 0, length: opConfig.size });
            const lastChunkSize = total % opConfig.size;
            if (lastChunkSize > 0) {
                slices.push({
                    total,
                    src,
                    ready,
                    length: lastChunkSize + delta,
                    offset: (numFullChunks * opConfig.size) - delta,
                });
            }
            // Mark the last slice so we know when to archive the file.
            slices[slices.length - 1].last = true;
        });
    }

    function decompress(src, dst) {
        return new Promise((resolve, reject) => {
            context.apis.assets.getPath('file-assets')
                .then((dir) => {
                    const proc = spawn(`${dir}/bin/decompress`, [src, dst], {});
                    let stderr = '';
                    proc.stderr.on('data', (data) => {
                        stderr += data;
                    });
                    let stdout = '';
                    proc.stdout.on('data', (data) => {
                        stdout += data;
                    });
                    proc.on('close', (code) => {
                        if (code > 0) {
                            logger.error({ code, stdout, stderr, src, dst }, 'failed to decompress');
                            reject(src);
                        } else {
                            resolve(dst);
                        }
                    });
                })
                .catch((err) => {
                    logger.error(err, 'failed to get asset directory');
                    reject(src);
                });
        });
    }

    function handleNewFile(src) {
        return new Promise((resolve) => {
            const basename = path.basename(src);
            const tmp = tmpPath(basename);
            const ready = readyPath(basename);
            filedb.add(src);
            decompress(src, tmp)
                .then(() => {
                    filedb.decompressed(src);
                    logger.info({ src, tmp }, 'decompressed');
                })
                .then(() => fse.rename(tmp, ready))
                .then(() => {
                    filedb.readied(src);
                    logger.info({ tmp, ready }, 'readied');
                    sliceFile(src, ready);
                    resolve(ready);
                })
                .catch((err) => {
                    // TODO: anything to clean up?
                    logger.error(err, { src, tmp, ready }, 'failed to ready');
                });
        });
    }

    let globbed = false;
    function onUpstreamGlobbed(globErr, paths) {
        if (globErr) {
            logger.err(globErr, 'glob error');
        }
        // function sequence(tasks, fn) {
        //     return tasks.reduce((promise, task) => promise.then(() => fn(task)), Promise.resolve());
        // }
        const isReady = src => (
            fse.exists(`${src}.ready`)
                .then(exists => exists && filedb.isNew(src))
        );
        // TODO: `slices` needs to be keyed by file so we can handleNewFile()
        // concurrently (map() instead of each() below).
        Promise.resolve(paths)
            .filter(isReady)
            .each(handleNewFile)
            .then((i) => {
                logger.warn(i, 'TODO: GLOBBED');
                globbed = true;
            });
    }

    const events = context.foundation.getEventEmitter();

    events.on('slice:success', (slice) => {
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
        if (request.last) {
            fse.unlink(request.ready)
                .then(() => {
                    const archive = archivePath(path.basename(request.src));
                    fse.rename(request.src, archive)
                        .then(() => {
                            fse.unlink(`${request.src}.ready`)
                                .then(() => {
                                    filedb.remove(request.src);
                                    filedb.write();
                                    logger.info(request, 'archived');
                                });
                        });
                });
        }
    });

    // TODO: filedb.write() on slicing complete event.

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

    return waitForFirstGlob()
        .then(() => [nextSlice])
        .catch(logger.err);
}

function newReader(context, opConfig) {
    return function processSlice(slice, logger) {
        async function reader(offset, length) {
            const fd = await fse.open(slice.ready, 'r');
            try {
                return fse.read(fd, Buffer.alloc(2 * opConfig.size), 0, length, offset)
                    .then(r =>
                        r.buffer.slice(0, r.bytesRead).toString())
                    .catch((err) => {
                        fse.close(fd);
                        throw err;
                    });
            } finally {
                fse.close(fd);
            }
        }
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
    parallelSlicers,
};
