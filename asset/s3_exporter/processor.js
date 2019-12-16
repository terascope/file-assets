'use strict';

const {
    BatchProcessor, getClient
} = require('@terascope/job-components');
const json2csv = require('json2csv').parse;
const { DataEntity } = require('@terascope/utils');
const lz4 = require('lz4');
const { gzip } = require('node-gzip');

class S3Batcher extends BatchProcessor {
    constructor(context, opConfig, executionConfig) {
        super(context, opConfig, executionConfig);
        this.client = getClient(context, opConfig, 's3');
        this.workerId = context.cluster.worker.id;
        this.objPrefix = opConfig.object_prefix;
        // This will be incremented as the worker processes slices and used as a way to create
        // unique object names
        this.sliceCount = 0;
    }

    // Implementing a custom stringify to pass nested objects through when fields are specified
    _stringify(record) {
        let serializedRecord = '{';
        this.opConfig.fields.forEach((field) => {
            // Can't just check for `record[field]` since `null`, `undefined`, and `0` will drop
            // fields
            if (Object.keys(record).includes(field)) {
                serializedRecord = `${serializedRecord}"${field}":${JSON.stringify(record[field])},`;
            }
        });
        return `${serializedRecord.slice(0, -1)}}`;
    }

    getName() {
        let objName;
        if (this.opConfig.compression === 'lz4') {
            objName = `${this.objPrefix}${this.workerId}.${this.sliceCount}.lz4`;
        } else if (this.opConfig.compression === 'gzip') {
            objName = `${this.objPrefix}${this.workerId}.${this.sliceCount}.gz`;
        } else {
            objName = `${this.objPrefix}${this.workerId}.${this.sliceCount}`;
        }
        this.sliceCount += 1;
        return objName;
    }

    async onBatch(slice) {
        const objName = this.getName();

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

        // Build the output string to dump to the object
        // TODO externalize this into a ./lib/ for use with the `file_exporter`
        let outStr = '';
        switch (this.opConfig.format) {
        case 'csv':
        case 'tsv':
            // null or empty slices will manifest as blank lines in the output file
            if (!slice || !slice.length) outStr = this.opConfig.line_delimiter;
            else outStr = `${json2csv(slice, csvOptions)}${this.opConfig.line_delimiter}`;
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
                    outStr = `${outStr}${this._stringify(record)}${this.opConfig.line_delimiter}`;
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

        // This will prevent empty objects from being added to the S3 store, which can cause
        // problems with the S3 reader
        if (outStr.length === 0) {
            return Promise.resolve();
        }

        const params = {
            Bucket: this.opConfig.bucket,
            Key: objName
        };

        if (this.opConfig.compression === 'lz4') {
            params.Body = lz4.encode(outStr);
        } else if (this.opConfig.compression === 'gzip') {
            params.Body = await gzip(outStr);
        } else {
            params.Body = outStr;
        }

        return this.client.putObject_Async(params)
            .then((results) => DataEntity.fromBuffer(JSON.stringify(results)));
    }
}

module.exports = S3Batcher;
