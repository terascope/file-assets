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
    getFieldsFromConfig,
} from '../interfaces';

type FormatFn = (
    slice: any[], config: ChunkedFileSenderConfig, csvOptions: json2csv.Options<any>
) => string

function csvFunction(
    slice: any[],
    config: ChunkedFileSenderConfig,
    csvOptions: json2csv.Options<any>
) {
    return parse(slice, csvOptions);
}

const formatsFns: Record<Format, FormatFn> = {
    csv: csvFunction,
    tsv: csvFunction,
    raw(slice, config) {
        const lineDelimiter = getLineDelimiter(config);
        return slice.map((record: any) => record.data).join(lineDelimiter);
    },
    ldjson(slice, config) {
        const lineDelimiter = getLineDelimiter(config);
        const fields = getFieldsFromConfig(config);
        return slice.map((record: any) => JSON.stringify(record, fields)).join(lineDelimiter);
    },
    json(slice, config) {
        const fields = getFieldsFromConfig(config);
        return JSON.stringify(slice, fields);
    }
};

function getFormatFn(format: Format): FormatFn {
    const fn = formatsFns[format];
    if (fn == null) throw new TSError(`Unsupported output format "${format}"`);
    return fn;
}

const formatValues = Object.values(Format);

export class Formatter {
    csvOptions: json2csv.Options<any>;
    private config: ChunkedFileSenderConfig;
    private fn: FormatFn;

    constructor(config: ChunkedFileSenderConfig) {
        this.validateConfig(config);
        this.config = { ...config };
        this.csvOptions = makeCSVOptions(config);
        this.fn = getFormatFn(config.format);
    }

    get type(): Format {
        return this.config.format;
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
     * Some formats, like ldjson and csv can be parsed by row,
     * so this function returns an iterator to format instead
     * of doing it all at once which can potentially throw invalid
     * string or buffer length errors
    */
    * formatIterator(slice: any[]): IterableIterator<string> {
        let firstSlice = true;
        for (const row of slice) {
            const lineDelimiter = getLineDelimiter(this.config);
            const formatted = this.fn([row], this.config, {
                ...this.csvOptions,
                header: this.csvOptions.header && firstSlice,
            });
            firstSlice = false;

            if (formatted.length) {
                yield formatted + lineDelimiter;
            } else {
                yield formatted;
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
        const lineDelimiter = getLineDelimiter(this.config);
        const formatted = this.fn(slice, this.config, this.csvOptions);
        if (!formatted.length) return '';
        return formatted + lineDelimiter;
    }
}

function makeCSVOptions(config: ChunkedFileSenderConfig): CSVOptions {
    if (!isCSVSenderConfig(config)) return {};

    return {
        fields: getFieldsFromConfig(config),
        header: config.include_header ?? false,
        eol: getLineDelimiter(config),
        delimiter: getFieldDelimiter(config),
    };
}
