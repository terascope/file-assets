'use strict';

const Promise = require('bluebird');
const Queue = require('@terascope/queue');
const path = require('path');
const { getChunk, getOffsets } = require('@terascope/chunked-file-reader');
const { TSError } = require('@terascope/utils');
const fse = require('fs-extra');

function newSlicer(context, executionContext, retryData, logger) {
    const opConfig = executionContext.config.operations[0];
    const events = context.foundation.getEventEmitter();
    checkProvidedPath(opConfig.path);
    const queue = new Queue();

    // Set up flags for slice generation/management
    let isFinished = false;
    let isShutdown = false;

    events.on('worker:shutdown', () => {
        isShutdown = true;
    });

    function checkProvidedPath(filePath) {
        try {
            const dirStats = fse.lstatSync(filePath);
            if (dirStats.isSymbolicLink()) {
                throw new Error('Directory for `file_reader` cannot be a symlink!');
            }
            const dirContents = fse.readdirSync(filePath);
            if (dirContents.length === 0) {
                throw new Error('Must provide a non-empty directory for `file_reader`!!');
            }
        } catch (err) {
            throw new Error(`Path must be valid! Encountered the following error:\n${err}`);
        }
    }

    async function processFile(filePath) {
        const stat = await fse.stat(filePath);
        const total = stat.size;
        // Override file slicing to make sure JSON files are not split across slices
        if (opConfig.format === 'json') {
            const offset = {};
            offset.path = filePath;
            offset.offset = 0;
            offset.length = total;
            queue.enqueue(offset);
        } else {
            getOffsets(opConfig.size, total, opConfig.line_delimiter).forEach((offset) => {
                offset.path = filePath;
                queue.enqueue(offset);
            });
        }
        return true;
    }

    async function getFilePaths(filePath) {
        const dirContents = await fse.readdir(filePath);
        return Promise.map(dirContents, async (file) => {
            const fullPath = path.join(filePath, file);
            const stats = await fse.lstat(fullPath);
            if (stats.isFile()) {
                return processFile(fullPath);
            }
            if (stats.isDirectory()) {
                return getFilePaths(fullPath);
            }
            logger.error(`${file} is not a file or directory!!`);
            return true;
        })
            // Catch the error and log it so the execution controller doesn't crash and burn if
            // there is a bad file or directory
            .catch((err) => {
                const error = new TSError(err, {
                    reason: 'Error while reading file or directory',
                    context: {
                        filePath
                    }
                });
                logger.error(error);
            });
    }

    getFilePaths(opConfig.path)
        .then(() => {
            isFinished = true;
        });

    return [() => {
        // Grab a record if there is one ready in the queue
        if (queue.size() > 0) return queue.dequeue();

        // Finish slicer if the queue is empty and it's done prepping slices
        if (isFinished) return null;

        // If the queue is empty, wait for a new slice or throw an error if the slicer shuts down
        // early
        return new Promise((resolve, reject) => {
            const intervalId = setInterval(() => {
                if (queue.size() > 0) {
                    clearInterval(intervalId);
                    resolve(queue.dequeue());
                }
                if (isShutdown) {
                    clearInterval(intervalId);
                    reject(new TSError('Slicer not finished but told to shutdown'));
                }
            }, 50);
        });
    }];
}


function newReader(context, opConfig) {
    // Coerce the field delimiter if the format is `tsv`
    if (opConfig.format === 'tsv') {
        opConfig.field_delimiter = '\t';
    }
    return function processSlice(slice, logger) {
        async function reader(offset, length) {
            const fd = await fse.open(slice.path, 'r');
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
        path: {
            doc: 'Directory that contains the files to process. If the directory consists of a mix '
                + 'of subdirectories and files, the slicer will crawl through the subdirectories to'
                + ' slice all of the files.',
            default: null,
            format: 'required_String'
        },
        size: {
            doc: 'Determines slice size in bytes',
            default: 10000000,
            format: Number
        },
        format: {
            doc: 'Format of the target file. Currently supports "json", "ldjson", "raw", "tsv", and'
                + ' "csv".',
            default: 'ldjson',
            format: ['json', 'ldjson', 'raw', 'tsv', 'csv']
        },
        field_delimiter: {
            doc: 'Delimiter character',
            default: ',',
            format: String
        },
        line_delimiter: {
            doc: 'Determines the line delimiter used in the file being read.',
            default: '\n',
            format: String
        },
        fields: {
            doc: 'CSV field headers used to create the json key, must be in same order as file',
            default: [],
            format: Array
        },
        remove_header: {
            doc: 'Checks for the header row and removes it',
            default: true,
            format: 'Boolean'
        }
    };
}


module.exports = {
    newReader,
    newSlicer,
    schema
};
