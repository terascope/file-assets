'use strict';

const Promise = require('bluebird');
const Queue = require('@terascope/queue');
const { getChunk, getOffsets } = require('@terascope/chunked-file-reader');
const fse = require('fs-extra');

function newSlicer(context, executionContext, retryData, logger) {
    const opConfig = executionContext.config.operations[0];
    const queue = new Queue();

    async function processFile(filePath) {
        const stat = await fse.stat(filePath);
        const total = stat.size;
        getOffsets(opConfig.size, total, opConfig.delimiter).forEach((offset) => {
            offset.path = filePath;
            queue.enqueue(offset);
        });
    }

    async function getFilePaths(filePath) {
        const dirContents = await fse.readdir(filePath);
        return Promise.map(dirContents, async (file) => {
            const fullPath = `${filePath}/${file}`;
            const stats = await fse.stat(fullPath);
            if (stats.isFile()) {
                return processFile(fullPath);
            }
            if (stats.isDirectory()) {
                return getFilePaths(fullPath);
            }
            logger.error(`${file} is not a file or directory!!`);
            return true;
        })
            .return([() => queue.dequeue()]);
    }

    return getFilePaths(opConfig.path)
        .catch((err) => {
            logger.error('Error while reading slicing files, error:', err);
            return Promise.reject(err);
        });
}


function newReader(context, opConfig) {
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
        return getChunk(reader, slice, opConfig, logger);
    };
}

function schema() {
    return {
        path: {
            doc: 'Directory that contains the files to process. If the directory consists of a mix '
                + 'of subdirectories and files, the slicer will crawl through the subdirectories to'
                + ' slice all of the files.',
            default: '',
            format: (val) => {
                if (typeof val !== 'string') {
                    throw new Error('path in file_reader must be a string!');
                }

                if (val.length === 0) {
                    throw new Error('path in file_reader must specify a valid path!');
                }
            }
        },
        delimiter: {
            doc: 'Determines the delimiter used in the file being read. '
                + 'Currently only supports "\n"',
            default: '\n',
            format: ['\n']
        },
        size: {
            doc: 'Determines slice size in bytes',
            default: 100000,
            format: Number
        },
        format: {
            doc: 'Format of the target file. Currently only supports "json"',
            default: 'json',
            format: ['json']
        }
    };
}


module.exports = {
    newReader,
    newSlicer,
    schema
};
