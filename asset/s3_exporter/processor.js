'use strict';

const {
    BatchProcessor, getClient
} = require('@terascope/job-components');
const { parseForFile } = require('../lib/parser');
const { getName, parsePath } = require('../lib/fileName');

class S3Batcher extends BatchProcessor {
    constructor(context, opConfig, executionConfig) {
        super(context, opConfig, executionConfig);
        this.client = getClient(context, opConfig, 's3');
        this.objPath = parsePath(opConfig.path);
        this.worker = context.cluster.worker.id;
        this.ext = opConfig.extension;
        // This will be incremented as the worker processes slices and used as a way to create
        // unique object names
        this.sliceCount = 0;
        // Allows this to use the externalized name builder
        this.opConfig.path = this.objPath.prefix;
    }

    async onBatch(slice) {
        const objName = getName(this.worker, this.sliceCount, this.opConfig);

        // Set the options for the parser
        const csvOptions = {
            header: this.opConfig.include_header,
            eol: this.opConfig.line_delimiter

        };
        // Only need to set `fields` if there is a custom list since the library will, by default,
        // use the record' top-level attributes. This might be a problem if records are missing
        // attirbutes
        if (this.opConfig.fields.length !== 0) {
            csvOptions.fields = this.opConfig.fields;
        }
        // Assumes a custom delimiter will be used only if the `csv` output format is chosen
        if (this.opConfig.format === 'csv') {
            csvOptions.delimiter = this.opConfig.field_delimiter;
        } else if (this.opConfig.format === 'tsv') {
            csvOptions.delimiter = '\t';
        }

        const outStr = await parseForFile(slice, this.opConfig, csvOptions);
        this.sliceCount += 1;

        // This will prevent empty objects from being added to the S3 store, which can cause
        // problems with the S3 reader
        if (outStr.length === 0) {
            return [];
        }

        const params = {
            Bucket: this.objPath.bucket,
            Key: objName,
            Body: outStr
        };

        return this.client.putObject_Async(params)
            .then(() => slice);
    }
}

module.exports = S3Batcher;
