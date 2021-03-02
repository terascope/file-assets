import { isNumber } from '@terascope/job-components';
import { Compression, Format } from '@terascope/file-asset-apis';

const readerSchema = {
    size: {
        doc: 'Determines the size of the slice in bytes',
        default: 10000000,
        format: Number
    },
    remove_header: {
        doc: 'Checks for the header row in csv or tsv files and removes it',
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
        doc: 'A configuration object used to pass in any extra csv parsing arguments',
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
            + ' "/data/export_hs897f.1079.gz"',
        default: null,
        format: 'required_String'
    },
    extension: {
        doc: 'A file extension override, by default an extension will be added to the file based on the format and compression settings',
        default: null,
        format: 'optional_String'
    },
    compression: {
        doc: 'Compression to use on the object. Supports lz4 and gzip.',
        default: Compression.none,
        format: Object.keys(Compression)
    },
    field_delimiter: {
        doc: 'Delimiter character between record fields. Only used with `csv` format',
        default: null,
        format: 'optional_String'
    },
    line_delimiter: {
        doc: 'Line delimiter character for the object',
        default: null,
        format: 'optional_String'
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
        format: (val: unknown): void => {
            if (!isNumber(val) || val <= 0) throw new Error('Invalid concurrency setting, it must be a number and greater than 0');
        }
    }
};

export const fileReaderSchema = Object.assign({}, commonSchema, readerSchema);

export const opSchema = {
    api_name: {
        doc: 'name of api to be used by operation',
        default: null,
        format: 'optional_String'
    }
};
