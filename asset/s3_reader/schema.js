'use strict';

const { ConvictSchema } = require('@terascope/job-components');

class Schema extends ConvictSchema {
    build() {
        return {
            bucket: {
                doc: 'The S3 bucket with objects to process',
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
                    + ' or objects and directories starting with the `object_prefix` if there is no tra'
                    + 'iling `/`',
                default: '',
                format: String
            },
            size: {
                doc: 'Determines slice size in bytes',
                default: 10000000,
                format: Number
            },
            format: {
                doc: 'Format of the target file. Currently supports "json", "ldjson", "raw", "tsv", and'
                    + ' "csv".',
                default: 'ldjson',
                format: ['json', 'ldjson', 'raw', 'tsv', 'csv']
            },
            field_delimiter: {
                doc: 'Delimiter character',
                default: ',',
                format: String
            },
            line_delimiter: {
                doc: 'Determines the line delimiter used in the file being read.',
                default: '\n',
                format: String
            },
            fields: {
                doc: 'CSV field headers used to create the json key, must be in same order as file',
                default: [],
                format: Array
            },
            remove_header: {
                doc: 'Checks for the header row and removes it',
                default: true,
                format: 'Boolean'
            },
            ignore_empty: {
                doc: 'Ignores fields without values when parsing CSV.\ni.e. the row "val1,,val3" '
                    + 'will generate the record \'{"field1":"val1","field3":"val3"}\' if set to '
                    + 'true',
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
