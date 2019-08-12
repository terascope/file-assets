'use strict';

const {
    BatchProcessor
} = require('@terascope/job-components');
const json2csv = require('json2csv').parse;
const Promise = require('bluebird');
const path = require('path');
const fse = require('fs-extra');
const { TSError } = require('@terascope/utils');

class FileBatcher extends BatchProcessor {
    constructor(context, opConfig, executionConfig) {
        super(context, opConfig, executionConfig);
        this.worker = context.cluster.worker.id;
        this.filePrefix = opConfig.file_prefix;
        if (opConfig.file_per_slice || (this.opConfig.format === 'json')) {
            this.filePerSlice = true;
        } else {
            this.filePerSlice = false;
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

    getName() {
        const fileName = path.join(this.opConfig.path, `${this.filePrefix}${this.worker}`);
        if (this.filePerSlice) {
            this.sliceCount += 1;
            // Slice count is only used in the file name with `file_per_slice`
            return `${fileName}.${this.sliceCount - 1}`;
        }
        if (!this.firstSlice) this.csvOptions.header = false;
        this.firstSlice = false;
        return fileName;
    }

    async onBatch(slice) {
        // console.log(slice)
        const fileName = this.getName();
        // console.log(fileName)

        // Build the output string to dump to the object
        // TODO externalize this into a ./lib/ for use with the `file_exporter`
        let outStr = '';
        switch (this.opConfig.format) {
        case 'csv':
        case 'tsv':
            // null or empty slices will manifest as blank lines in the output file
            if (!slice || !slice.length) outStr = this.opConfig.line_delimiter;
            else outStr = `${json2csv(slice, this.csvOptions)}${this.opConfig.line_delimiter}`;
            break;
        case 'raw': {
            slice.forEach((record) => {
                outStr = `${outStr}${record.data}${this.opConfig.line_delimiter}`;
            });
            break;
        }
        case 'ldjson': {
            if (this.opConfig.fields.length > 0) {
                slice.forEach((record) => {
                    outStr = `${outStr}${JSON.stringify(record, this.opConfig.fields)}${this.opConfig.line_delimiter}`;
                });
            } else {
                slice.forEach((record) => {
                    outStr = `${outStr}${JSON.stringify(record)}${this.opConfig.line_delimiter}`;
                });
            }
            break;
        }
        case 'json': {
            // This case assumes the data is just a single record in the slice's data array. We
            // could just strigify the slice as-is, but feeding the output back into the reader
            // would just nest that array into a record in that slice's array, which probably
            // isn't the desired effect.
            outStr = `${JSON.stringify(slice)}${this.opConfig.line_delimiter}`;
            break;
        }
        // Schema validation guards against this
        default:
            throw new Error(`Unsupported output format "${this.opConfig.format}"`);
        }

        // console.log(outStr)

        // Doesn't return a DataEntity or anything else if siccessful
        return fse.appendFile(fileName, outStr)
            .catch((err) => Promise.reject(new TSError(err, {
                reason: `Failure to append to file ${fileName}`
            })));
    }
}

module.exports = FileBatcher;
