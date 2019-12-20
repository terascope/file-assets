'use strict';

const { ConvictSchema } = require('@terascope/job-components');

class Schema extends ConvictSchema {
    build() {
        return {
            bucket: {
                doc: 'The bucket where the processed data will live',
                default: null,
                format: 'required_String'
            },
            connection: {
                doc: 'The S3 connection from Terafoundation to use',
                default: null,
                format: 'required_String'
            },
            compression: {
                doc: 'Compression to use on the object. Supports lz4 and gzip.',
                default: 'none',
                format: ['none', 'lz4', 'gzip']
            },
            object_prefix: {
                doc: 'The object prefix. Will target a specific directory if a trailing `/` is provided'
                    + '. See docs for more info',
                default: '',
                format: String
            },
            format: {
                doc: 'Format of the target object. Currently supports "json", "ldjson", "raw", "tsv", and'
                    + ' "csv".',
                default: 'ldjson',
                format: ['json', 'ldjson', 'raw', 'tsv', 'csv']
            },
            field_delimiter: {
                doc: 'Delimiter character between record fields. Only used with `csv` format',
                default: ',',
                format: String
            },
            line_delimiter: {
                doc: 'Line delimiter character for the object',
                default: '\n',
                format: String
            },
            fields: {
                doc: 'CSV field headers used to create the json key, must be in same order as the',
                default: [],
                format: Array
            },
            include_header: {
                doc: 'Determines whether or not to include column headers for the fields in the obj'
                    + 'ects',
                default: false,
                format: 'Boolean'
            },
            object_per_slice: {
                doc: 'Determines whether to batch slices in a multi-part upload or not. This '
                    + 'capability will be included in a future improvement',
                default: 'false',
                format: [false]
            }
        };
    }
}

module.exports = Schema;
