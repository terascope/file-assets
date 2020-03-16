import { isNumber } from '@terascope/utils';
import { Compression, Format, CSVOptions } from './interfaces';

// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface FileConfig {
    path: string;
    extension: string;
    compression: Compression;
    field_delimiter: string;
    line_delimiter: string;
    fields: string[];
    file_per_slice: boolean;
    include_header: boolean;
    format: Format;
}

// TODO: include_header vs remove_header, can they be unified??
export interface ReaderFileConfig extends FileConfig {
    size: number;
    connection: string;
    remove_header: boolean;
    ignore_empty: boolean;
    extra_args: CSVOptions;
}

const readerSchema = {
    size: {
        doc: 'Determines slice size in bytes',
        default: 10000000,
        format: Number
    },
    remove_header: {
        doc: 'Checks for the header row and removes it',
        default: true,
        format: 'Boolean'
    },
    ignore_empty: {
        doc: 'Ignores fields without values when parsing CSV.\ni.e. the row "val1,val3" '
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
    connection: {
        doc: 'The connection from Terafoundation to use',
        default: 'default',
        format: 'optional_String'
    },
};

export const commonSchema = {
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
        default: true,
        format: Boolean
    },
    include_header: {
        doc: 'Determines whether or not to include column headers for the fields.',
        default: false,
        format: Boolean
    },
    format: {
        doc: 'Format of the target object. Currently supports "json", "ldjson", "raw", "tsv", and'
            + ' "csv".',
        default: Format.ldjson,
        format: Object.keys(Format)
    },
    concurrency: {
        doc: 'the number of in flight actions',
        default: 10,
        format: (val: any) => {
            if (!isNumber(val) || val <= 0) throw new Error('Invalid concurrency setting, it must be a number and greater than 0');
        }
    }
};

export const fileReaderSchema = Object.assign({}, commonSchema, readerSchema);
