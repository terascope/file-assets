'use strict';

const { ConvictSchema } = require('@terascope/job-components');

class Schema extends ConvictSchema {
    build() {
        return {
            path: {
                doc: 'Directory that contains the files to process. If the directory consists of a '
                    + 'mix of subdirectories and files, the slicer will crawl through the '
                    + 'subdirectories to slice all of the files.',
                default: null,
                format: 'required_String'
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
