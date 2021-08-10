import {
    TSError,
    isNil,
    isString,
    getTypeOf,
    toHumanTime,
    isTest,
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
    SendRecords,
    SendRecord,
} from '../interfaces';

export type FormatterOptions = Omit<ChunkedFileSenderConfig, 'id'|'path'|'compression'|'file_per_slice'>;

type MakeFormatFn = (
    config: FormatterOptions, csvOptions: json2csv.Options<any>
) => FormatFn

type FormatFn = (
    slice: SendRecords, isFirstSlice: boolean
) => string

function makeCSVOrTSVFunction(
    _config: FormatterOptions, csvOptions: json2csv.Options<any>
): FormatFn {
    return function csvOrTSVFormat(
        slice: SendRecords,
        isFirstSlice: boolean
    ) {
        return parse(slice, {
            ...csvOptions,
            header: csvOptions.header && isFirstSlice,
        });
    };
}

function makeRawFunction(config: FormatterOptions): FormatFn {
    const lineDelimiter = getLineDelimiter(config);
    return function rawFormat(
        slice: SendRecords,
    ) {
        return slice.map((record) => record.data).join(lineDelimiter);
    };
}

function makeLDJSONFunction(config: FormatterOptions): FormatFn {
    const lineDelimiter = getLineDelimiter(config);
    const fields = getFieldsFromConfig(config);
    function _stringify(record: SendRecord): string {
        return JSON.stringify(record, fields);
    }
    return function ldjsonFormat(
        slice: SendRecords,
    ) {
        let output = '';
        const len = slice.length;
        for (let i = 0; i < len; i++) {
            output += _stringify(slice[i]);
            if ((i + 1) < len) {
                output += lineDelimiter;
            }
        }
        return output;
    };
}

function makeJSONFunction(config: FormatterOptions): FormatFn {
    const fields = getFieldsFromConfig(config);
    return function jsonFormat(
        slice: SendRecords,
    ) {
        return JSON.stringify(slice, fields);
    };
}

const formatsFns: Record<Format, MakeFormatFn> = {
    csv: makeCSVOrTSVFunction,
    tsv: makeCSVOrTSVFunction,
    raw: makeRawFunction,
    ldjson: makeLDJSONFunction,
    json: makeJSONFunction
};

function getFormatFn(format: Format): MakeFormatFn {
    const fn = formatsFns[format];
    if (fn == null) throw new TSError(`Unsupported output format "${format}"`);
    return fn;
}

const formatValues = Object.values(Format);

const CHUNK_SIZE = isTest ? 10 : 100;

export class Formatter {
    csvOptions: json2csv.Options<any>;
    private config: FormatterOptions;
    private fn: FormatFn;

    constructor(config: FormatterOptions) {
        this.validateConfig(config);
        this.config = { ...config };
        this.csvOptions = makeCSVOptions(config);
        this.fn = getFormatFn(config.format)(this.config, this.csvOptions);
    }

    get type(): Format {
        return this.config.format;
    }

    private validateConfig(config: FormatterOptions) {
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
    * formatIterator(slice: SendRecords): IterableIterator<[formatted: string, has_more: boolean]> {
        let firstSlice = true;

        const lineDelimiter = getLineDelimiter(this.config);
        let totalTime = 0;

        for (const [chunk, has_more] of chunkIterator(slice, CHUNK_SIZE)) {
            const start = Date.now();

            const formatted = this.fn(chunk, firstSlice);
            firstSlice = false;

            totalTime += Date.now() - start;

            if (formatted.length) {
                yield [formatted + lineDelimiter, has_more];
            } else {
                yield [formatted, has_more];
            }
        }

        console.log(`formatIterator total time executing ${toHumanTime(totalTime)}`);
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
    format(slice: SendRecords): string {
        const lineDelimiter = getLineDelimiter(this.config);
        const formatted = this.fn(slice, true);
        if (!formatted.length) return '';
        return formatted + lineDelimiter;
    }
}

export function* chunkIterator<T>(
    dataArray: T[]|readonly T[], size: number
): IterableIterator<[data: T[], has_more: boolean]> {
    const len = dataArray.length;
    for (let i = 0; i < len; i += size) {
        const chunk = dataArray.slice(i, i + size);
        yield [chunk, (i + size) < len];
    }
}

function makeCSVOptions(config: FormatterOptions): CSVOptions {
    if (!isCSVSenderConfig(config)) return {};

    return {
        fields: getFieldsFromConfig(config),
        header: config.include_header ?? false,
        eol: getLineDelimiter(config),
        delimiter: getFieldDelimiter(config),
    };
}
