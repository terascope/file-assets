'use strict';

const {
    BatchProcessor
} = require('@terascope/job-components');
const Promise = require('bluebird');
const fse = require('fs-extra');
const { TSError } = require('@terascope/utils');
const { getName } = require('../_lib/fileName');
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
        this.sliceCount = 0;
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
        const fileName = getName(this.worker, this.sliceCount, this.opConfig);

        if (!this.opConfig.file_per_slice) {
            if (!this.firstSlice) this.csvOptions.header = false;
            this.firstSlice = false;
        }

        const outStr = await parseForFile(slice, this.opConfig, this.csvOptions);
        this.sliceCount += 1;

        // Prevents empty slices from resulting in empty files
        if (outStr === null) {
            return [];
        }

        // Doesn't return a DataEntity or anything else if siccessful
        return fse.appendFile(fileName, outStr)
            .catch((err) => Promise.reject(new TSError(err, {
                reason: `Failure to append to file ${fileName}`
            })));
    }
}

module.exports = FileBatcher;
