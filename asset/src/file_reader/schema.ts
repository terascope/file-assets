import { ConvictSchema } from '@terascope/job-components';
import { FileConfig } from './interfaces';

export default class Schema extends ConvictSchema<FileConfig> {
    build() {
        return {
            path: {
                doc: 'Directory that contains the files to process. If the directory consists of a '
                    + 'mix of subdirectories and files, the slicer will crawl through the '
                    + 'subdirectories to slice all of the files.',
                default: null,
                format: 'required_String'
            },
            compression: {
                doc: 'Compression used on the files. Supports lz4 and gzip.',
                default: 'none',
                format: ['none', 'lz4', 'gzip']
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
            },
            file_per_slice: {
                doc: 'Determines if each file should be treated as a single slice. If set, `size`'
                    + ' will be ignored.',
                default: false,
                format: Boolean
            }
        };
    }
}
