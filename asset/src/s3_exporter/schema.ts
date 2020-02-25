import { ConvictSchema } from '@terascope/job-components';
import { S3ExportConfig } from './interfaces';

export default class Schema extends ConvictSchema<S3ExportConfig> {
    build() {
        return {
            path: {
                doc: 'The bucket and prefix where objects should be placed. The object names will '
                    + 'be appended to this, so if no trailing "/" is provided, the final part will '
                    + 'be treated as a file prefix.\ni.e. "/data/export_" will result in files like'
                    + ' "/data/export_X7eLvcvd.1079.gz"',
                default: null,
                format: (objPath: any) => {
                    // Only throw an error if nothing is provided.
                    if (!objPath) throw new Error('Must include a bucket!');
                }
            },
            extension: {
                doc: 'A file extension to add to the object name.',
                default: '',
                format: String
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
                doc: 'Fields to include in the output',
                default: [],
                format: Array
            },
            include_header: {
                doc: 'Determines whether or not to include column headers for the fields.'
                    + 'ects',
                default: false,
                format: 'Boolean'
            },
            file_per_slice: {
                doc: 'Determines whether to batch slices in a multi-part upload or not. This '
                    + 'capability will be included in a future improvement',
                default: 'false',
                format: [false]
            }
        };
    }
}
