'use strict';

const { ConvictSchema } = require('@terascope/job-components');

class Schema extends ConvictSchema {
    build() {
        return {
            path: {
                doc: 'Path to the file where the data will be saved to, directory must pre-exist.',
                default: null,
                format: 'required_String'
            },
            file_prefix: {
                doc: 'Optional prefix to prepend to the file name.',
                default: 'export_',
                format: String
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
            file_per_slice: {
                doc: 'Determines if a new file is created for each slice.',
                default: false,
                format: Boolean
            },
            include_header: {
                doc: 'Determines whether or not to include a header at the top of the file. '
                + 'The header will consist of the field names.',
                default: false,
                format: 'Boolean'
            },
            format: {
                doc: 'Format of the target object. Currently supports "json", "ldjson", "raw", "tsv", and'
                    + ' "csv".',
                default: 'ldjson',
                format: ['json', 'ldjson', 'raw', 'tsv', 'csv']
            }
        };
    }
}

module.exports = Schema;
