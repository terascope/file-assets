import {
    TSError,
    isNil,
    isString,
    getTypeOf
} from '@terascope/utils';
import json2csv, { parse } from 'json2csv';
import {
    Format,
    CSVOptions,
    ChunkedFileSenderConfig,
    isCSVSenderConfig,
    getLineDelimiter,
    getFieldDelimiter,
} from '../interfaces';

type FormatFn = (
    slice: any[], config: ChunkedFileSenderConfig, csvOptions: json2csv.Options<any>
) => string

function csvFunction(
    slice: any[],
    config: ChunkedFileSenderConfig,
    csvOptions: json2csv.Options<any>
) {
    const lineDelimiter = getLineDelimiter(config);
    return `${parse(slice, csvOptions)}${lineDelimiter}`;
}

const formatsFns: Record<Format, FormatFn> = {
    csv: csvFunction,
    tsv: csvFunction,
    raw(slice, config) {
        const lineDelimiter = getLineDelimiter(config);
        return `${slice.map((record: any) => record.data).join(lineDelimiter)}${lineDelimiter}`;
    },
    ldjson(slice, config) {
        const lineDelimiter = getLineDelimiter(config);
        const fields = getFields(config);
        return `${slice.map((record: any) => JSON.stringify(record, fields)).join(lineDelimiter)}${lineDelimiter}`;
    },
    json(slice, config) {
        const lineDelimiter = getLineDelimiter(config);
        const fields = getFields(config);
        return `${JSON.stringify(slice, fields)}${lineDelimiter}`;
    }
};

function getFields(config: ChunkedFileSenderConfig): string[]|undefined {
    const fields = (config as any).fields as string[]|undefined;
    return fields?.length ? fields : undefined;
}

function getFormatFn(format: Format): FormatFn {
    const fn = formatsFns[format];
    if (fn == null) throw new TSError(`Unsupported output format "${format}"`);
    return fn;
}

const formatValues = Object.values(Format);

export class FileFormatter {
    csvOptions: json2csv.Options<any>;
    private config: ChunkedFileSenderConfig;
    private fn: FormatFn;

    constructor(config: ChunkedFileSenderConfig) {
        this.validateConfig(config);
        this.config = { ...config };
        this.csvOptions = makeCsvOptions(config);
        this.fn = getFormatFn(config.format);
    }

    private validateConfig(config: ChunkedFileSenderConfig) {
        const { line_delimiter, format } = config;
        if (!formatValues.includes(format)) {
            throw new TSError(`Unsupported output format "${format}"`);
        }
        if (line_delimiter != null && !isString(line_delimiter)) {
            throw new TSError(`Invalid parameter line_delimiter, it must be provided and be of type string, was given ${getTypeOf(config.line_delimiter)}`);
        }
        if (isCSVSenderConfig(config)) {
            if (!isNil(config.fields) && (
                !Array.isArray(config.fields) || !config.fields.every(isString)
            )) {
                throw new TSError(`Invalid parameter fields, it must be provided and be an empty array or an array of strings, was given ${getTypeOf(config.fields)}`);
            }
        }
    }

    /**
     * Formats data based on configuration
     *
     * json => will stringify the whole array
     *
     * ldjson => will stringify each individual record and separate them by a new line
     *
     * csv/tsv => will convert data to comma or tab separated columns format
     *
     * raw => writes raw data as is, requires the use of DataEntity raw data.
     */
    format(slice: any[]): string {
        return this.fn(slice, this.config, this.csvOptions);
    }
}

function makeCsvOptions(config: ChunkedFileSenderConfig): CSVOptions {
    if (!isCSVSenderConfig(config)) return {};

    const csvOptions: CSVOptions = {};

    if (config.fields?.length !== 0) {
        csvOptions.fields = config.fields;
    } else {
        csvOptions.fields = undefined;
    }

    csvOptions.header = config.include_header;
    csvOptions.eol = getLineDelimiter(config);
    csvOptions.delimiter = getFieldDelimiter(config);

    return csvOptions;
}
