import { ConvictSchema } from '@terascope/job-components';
import { Compression, Format, FileExporterConfig } from './interfaces';

export default class Schema extends ConvictSchema<FileExporterConfig> {
    build() {
        return {
            path: {
                doc: 'Path to the file where the data will be saved to. The filename will be '
                    + 'appended to this, so if no trailing "/" is provided, the final part will '
                    + 'be treated as a file prefix.\ni.e. "/data/export_" will result in files like'
                    + ' "/data/export_X7eLvcvd.1079.gz"',
                default: null,
                format: 'required_String'
            },
            extension: {
                doc: 'A file extension to add to the object name.',
                default: '',
                format: String
            },
            compression: {
                doc: 'Compression to use on the object. Supports lz4 and gzip.',
                default: Compression.none,
                format: Object.keys(Compression)
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
                doc: 'Fields to include in the output',
                default: [],
                format: Array
            },
            file_per_slice: {
                doc: 'Determines if a new file is created for each slice.',
                default: false,
                format: Boolean
            },
            include_header: {
                doc: 'Determines whether or not to include column headers for the fields.',
                default: false,
                format: 'Boolean'
            },
            format: {
                doc: 'Format of the target object. Currently supports "json", "ldjson", "raw", "tsv", and'
                    + ' "csv".',
                default: Format.ldjson,
                format: Object.keys(Format)
            }
        };
    }
}
