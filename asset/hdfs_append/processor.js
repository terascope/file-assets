'use strict';

const {
    BatchProcessor, getClient
} = require('@terascope/job-components');
const { TSError } = require('@terascope/utils');
const Promise = require('bluebird');
const { parseForFile } = require('../_lib/parser');
const { batchSlice } = require('../_lib/slice');
const { getName } = require('../_lib/fileName');

class HDFSBatcher extends BatchProcessor {
    constructor(context, opConfig, executionConfig) {
        super(context, opConfig, executionConfig);
        // Client connection cannot be cached, an endpoint needs to be re-instantiated for a
        // different namenode_host
        opConfig.connection_cache = false;
        this.client = getClient(context, opConfig, 'hdfs_ha').client;
        this.worker = context.cluster.worker.id;
        this.ext = opConfig.extension;
        // This will be incremented as the worker processes slices and used as a way to create
        // unique object names. Set to -1 so it can be incremented before any slice processing is
        // done
        this.sliceCount = -1;

        // The append error detection and name change system need to be reworked to be compatible
        // with the file batching. In the meantime, restarting the job will sidestep the issue with
        // new worker names.
        // this.appendErrors = {};
    }

    async onBatch(slice) {
        // TODO also need to chunk the batches for multipart uploads
        const batches = batchSlice(slice, this.opConfig.path);

        // Needs to be incremented before slice processing so it increments consistently for a given
        // directory
        this.sliceCount += 1;

        return Promise.map(Object.keys(batches), async (path) => {
            const fileName = getName(
                this.worker,
                this.sliceCount,
                this.opConfig,
                path
            );
            // Set the options for the parser
            const csvOptions = {
                header: this.opConfig.include_header,
                eol: this.opConfig.line_delimiter

            };
            // Only need to set `fields` if there is a custom list since the library will, by
            // default, use the record' top-level attributes. This might be a problem if records are
            // missing attirbutes
            if (this.opConfig.fields.length !== 0) {
                csvOptions.fields = this.opConfig.fields;
            }
            // Assumes a custom delimiter will be used only if the `csv` output format is chosen
            if (this.opConfig.format === 'csv') {
                csvOptions.delimiter = this.opConfig.field_delimiter;
            } else if (this.opConfig.format === 'tsv') {
                csvOptions.delimiter = '\t';
            }

            const outStr = await parseForFile(batches[path], this.opConfig, csvOptions);

            // This will prevent empty objects from being added to the S3 store, which can cause
            // problems with the S3 reader
            if (!outStr || outStr.length === 0) {
                return [];
            }

            return this.client.getFileStatusAsync(fileName)
                .catch(() => this.client.mkdirsAsync(path.dirname(fileName))
                    .then(() => this.client.createAsync(fileName, ''))
                    .catch((err) => Promise.reject(new TSError(err, {
                        reason: 'Error while attempting to create a file',
                        context: {
                            fileName
                        }
                    }))))
                .return(outStr)
                .then((data) => this.client.appendAsync(fileName, data))
                .catch((err) => {
                    throw new TSError(err, {
                        reason: 'Error sending data to file',
                        context: {
                            file: fileName
                        }
                    });
                });
        });
    }
}

module.exports = HDFSBatcher;
