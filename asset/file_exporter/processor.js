'use strict';

const {
    BatchProcessor
} = require('@terascope/job-components');
const Promise = require('bluebird');
const fse = require('fs-extra');
const { TSError } = require('@terascope/utils');
const { getName } = require('../_lib/fileName');
const { batchSlice } = require('../_lib/slice');
const { parseForFile } = require('../_lib/parser');

class FileBatcher extends BatchProcessor {
    constructor(context, opConfig, executionConfig) {
        super(context, opConfig, executionConfig);
        this.worker = context.cluster.worker.id;
        // Coerce `file_per_slice` for JSON format or compressed output
        if ((opConfig.format === 'json') || (opConfig.compression !== 'none')) {
            this.opConfig.file_per_slice = true;
        }
        // Used for incrementing file name with `file_per_slice`
        this.sliceCount = -1;
        this.firstSlice = true;
        // Set the options for the parser
        this.csvOptions = {};
        if (this.opConfig.fields.length !== 0) {
            this.csvOptions.fields = this.opConfig.fields;
        } else {
            this.csvOptions.fields = null;
        }

        this.csvOptions.header = this.opConfig.include_header;
        this.csvOptions.eol = this.opConfig.line_delimiter;

        // Assumes a custom delimiter will be used only if the `csv` output format is chosen
        if (this.opConfig.format === 'csv') {
            this.csvOptions.delimiter = this.opConfig.field_delimiter;
        } else if (opConfig.format === 'tsv') {
            this.csvOptions.delimiter = '\t';
        }
    }

    async onBatch(slice) {
        // TODO also need to chunk the batches for multipart uploads
        const batches = batchSlice(slice, this.opConfig.path);

        this.sliceCount += 1;

        if (!this.opConfig.file_per_slice) {
            if (this.sliceCount > 0) this.csvOptions.header = false;
        }

        return Promise.map(Object.keys(batches), async (path) => {
            const fileName = getName(this.worker, this.sliceCount, this.opConfig, path);

            const outStr = await parseForFile(batches[path], this.opConfig, this.csvOptions);

            // Prevents empty slices from resulting in empty files
            if (!outStr || outStr.length === 0) {
                return [];
            }

            // Doesn't return a DataEntity or anything else if successful
            return fse.appendFile(fileName, outStr)
                .catch((err) => Promise.reject(new TSError(err, {
                    reason: `Failure to append to file ${fileName}`
                })));
        }).then(() => slice);
    }
}

module.exports = FileBatcher;
