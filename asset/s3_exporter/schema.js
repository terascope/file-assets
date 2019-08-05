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
            },
            ignore_empty: {
                doc: 'Only used with CSV parsing. Ignores any columns not specified in field list. '
                    + 'Since the field list is applied to columns sequentially, this will ignore '
                    + 'any additional columns past the number specified.\n'
                    + 'i.e. If 5 fields are specified, but there are 7 columns in the file, columns'
                    + ' 6 and 7 will be dropped if this is true. Otherwise, the parser will give '
                    + 'the fields generic names.',
                default: true,
                format: Boolean
            },
            extra_args: {
                doc: 'An object used to pass in any extra csv parsing arguments',
                default: {},
                format: Object
            }
        };
    }
}

module.exports = Schema;
