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
            }
        };
    }
}

module.exports = Schema;
