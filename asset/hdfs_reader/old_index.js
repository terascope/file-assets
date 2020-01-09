'use strict';

const path = require('path');
const Promise = require('bluebird');
const Queue = require('@terascope/queue');
const chunkReader = require('@terascope/chunked-file-reader');
const { TSError } = require('@terascope/utils');

function getClient(context, config, type) {
    const clientConfig = {};
    clientConfig.type = type;

    if (config && config.connection) {
        clientConfig.endpoint = config.connection ? config.connection : 'default';
        clientConfig.cached = config.connection_cache !== undefined
            ? config.connection_cache : true;
    } else {
        clientConfig.endpoint = 'default';
        clientConfig.cached = true;
    }

    return context.foundation.getConnection(clientConfig);
}

const parallelSlicers = false;

function newSlicer(context, executionContext) {
    const opConfig = executionContext.config.operations[0];
    const clientService = getClient(context, opConfig, 'hdfs_ha');
    const hdfsClient = clientService.client;
    const queue = new Queue();

    function processFile(file, filePath) {
        const totalLength = file.length;
        let fileSize = file.length;

        if (fileSize <= opConfig.size) {
            const fullFilePath = path.join(filePath, file.pathSuffix);
            queue.enqueue({ path: fullFilePath, fullChunk: true });
        } else {
            let offset = 0;
            while (fileSize > 0) {
                const length = fileSize > opConfig.size ? opConfig.size : fileSize;
                // This grabs the character immediately before the chunk to help check whether or
                // not the chunk starts with a complete record. If this is done, the length also
                // needs to increase by 1 to ensure no data is lost
                let startSpot = offset;
                let readLength = length;
                if (offset > 0) {
                    startSpot = offset - 1;
                    readLength = length + 1;
                }
                queue.enqueue({
                    path: path.join(filePath, file.pathSuffix),
                    offset: startSpot,
                    length: readLength,
                    total: totalLength
                });
                fileSize -= opConfig.size;
                offset += length;
            }
        }
    }

    function getFilePaths(filePath) {
        return hdfsClient.listStatusAsync(filePath)
            .then((results) => Promise.map(results, (metadata) => {
                if (metadata.type === 'FILE') {
                    return processFile(metadata, filePath);
                }
                if (metadata.type === 'DIRECTORY') {
                    const fullFilePath = path.join(filePath, metadata.pathSuffix);
                    return getFilePaths(fullFilePath);
                }
                return true;
            }))
            .return([() => queue.dequeue()])
            .catch((err) => Promise.reject(new TSError(err)));
    }

    return getFilePaths(opConfig.path)
        .catch((err) => Promise.reject(new TSError(err)));
}

function newReader(context, opConfig) {
    const logger = context.apis.foundation.makeLogger({ module: 'hdfs_reader' });
    const clientService = getClient(context, opConfig, 'hdfs_ha');
    const hdfsClient = clientService.client;

    return function processSlice(slice) {
        function reader(offset, length) {
            const opts = {
                offset,
                length
            };
            return hdfsClient.openAsync(slice.path, opts);
        }
        return chunkReader.getChunk(reader, slice, opConfig, logger, slice)
            .catch((err) => Promise.reject(new TSError(err)));
    };
}

function schema() {
    return {
        user: {
            doc: 'User to use when reading the files. Default: "hdfs"',
            default: 'hdfs',
            format: 'optional_String'
        },
        path: {
            doc: 'HDFS location to process. Most of the time this will be a directory that contains'
                + ' multiple files',
            default: '',
            format: (val) => {
                if (typeof val !== 'string') {
                    throw new Error('path in hdfs_reader must be a string!');
                }

                if (val.length === 0) {
                    throw new Error('path in hdfs_reader must specify a valid path in hdfs!');
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
        },
        connection: {
            doc: 'Name of the HDFS connection to use.',
            default: 'default',
            format: 'optional_String'
        }
    };
}

module.exports = {
    newReader,
    newSlicer,
    schema,
    parallelSlicers
};
