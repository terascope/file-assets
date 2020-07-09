import {
    TSError, isNil, isString, getTypeOf
} from '@terascope/job-components';
import json2csv, { parse } from 'json2csv';
import { Format, ParseOptions } from './interfaces';

type FormatFn = (slice: any[], opConfig: ParseOptions, csvOptions: json2csv.Options<any>) => string

function csvFunction(slice: any[], opConfig: ParseOptions, csvOptions: json2csv.Options<any>) {
    return `${parse(slice, csvOptions)}${opConfig.line_delimiter}`;
}

const formatsFns = {
    csv: csvFunction,
    tsv: csvFunction,
    raw(slice: any[], opConfig: ParseOptions) {
        return `${slice.map((record: any) => record.data).join(opConfig.line_delimiter)}${opConfig.line_delimiter}`;
    },
    ldjson(slice: any[], opConfig: ParseOptions) {
        return `${slice.map((record: any) => JSON.stringify(record, (opConfig.fields.length > 0) ? opConfig.fields : undefined)).join(opConfig.line_delimiter)}${opConfig.line_delimiter}`;
    },
    json(slice: any[], opConfig: ParseOptions) {
        return `${JSON.stringify(slice)}${opConfig.line_delimiter}`;
    }
};

function getFormatFn(format: Format): FormatFn {
    const fn = formatsFns[format];
    if (fn == null) throw new TSError(`Unsupported output format "${format}"`);
    return fn;
}

const formatValues = Object.values(Format);

export default class FileFormatter {
    csvOptions: json2csv.Options<any>;
    private config: ParseOptions;
    private fn: FormatFn;

    constructor(format: Format, config: ParseOptions, csvOptions: json2csv.Options<any> = {}) {
        this.validateConfig(format, config);
        this.config = config;
        this.csvOptions = csvOptions;
        this.fn = getFormatFn(format);
    }

    private validateConfig(format: Format, config: ParseOptions) {
        const { fields, line_delimiter } = config;
        if (!formatValues.includes(format)) throw new TSError(`Unsupported output format "${format}"`);
        if (isNil(line_delimiter) || !isString(line_delimiter)) throw new TSError(`Invalid parameter line_delimiter, it must be provided and be of type string, was given ${getTypeOf(config.line_delimiter)}`);
        if (format === Format.ldjson) {
            if (isNil(fields) || !Array.isArray(fields) || !fields.every(isString)) {
                throw new TSError(`Invalid parameter fields, it must be provided and be an empty array or an array of strings, was given ${getTypeOf(config.fields)}`);
            }
        }
    }

    format(slice: any[]): string {
        return this.fn(slice, this.config, this.csvOptions);
    }
}
