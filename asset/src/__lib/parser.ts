import json2csv, { parse } from 'json2csv';
import { DataEntity } from '@terascope/utils';
import { compress } from './compression';

export type CsvOptions = json2csv.Options<any>;

export enum Format {
    json = 'json',
    ldjson = 'ldjson',
    raw = 'raw',
    tsv = 'tsv',
    csv = 'csv',
}

export function makeCsvOptions(config: any) {
    const csvOptions: CsvOptions = {};

    if (config.fields.length !== 0) {
        csvOptions.fields = config.fields;
    } else {
        csvOptions.fields = undefined;
    }

    csvOptions.header = config.include_header;
    csvOptions.eol = config.line_delimiter;

    // Assumes a custom delimiter will be used only if the `csv` output format is chosen
    if (config.format === 'csv') {
        csvOptions.delimiter = config.field_delimiter;
    } else if (config.format === 'tsv') {
        csvOptions.delimiter = '\t';
    }
    return csvOptions;
}

const csv = (slice: any, opConfig: any, csvOptions: json2csv.Options<any>) => `${parse(slice, csvOptions)}${opConfig.line_delimiter}`;
const raw = (slice: any, opConfig: any) => `${slice.map((record: any) => record.data).join(opConfig.line_delimiter)}${opConfig.line_delimiter}`;
const ldjson = (slice: any, opConfig: any) => `${slice.map((record: any) => JSON.stringify(record, (opConfig.fields.length > 0) ? opConfig.fields : undefined)).join(opConfig.line_delimiter)}${opConfig.line_delimiter}`;
const json = (slice: any, opConfig: any) => `${JSON.stringify(slice)}${opConfig.line_delimiter}`;

export async function parseForFile(slice: DataEntity[], opConfig: any, csvOptions: CsvOptions) {
    // null or empty slices get an empty output and will get filtered out below
    if (!slice || !slice.length) return null;
    // Build the output string to dump to the object
    // TODO externalize this into a ./lib/ for use with the `file_exporter`
    // let outStr = '';

    const parseData = (format: string) => {
        const formats = {
            csv,
            tsv: csv,
            raw,
            ldjson,
            json,
            default: () => {
                throw new Error(`Unsupported output format "${opConfig.format}"`);
            }
        };
        return (formats[format] || formats.default)(slice, opConfig, csvOptions);
    };

    const outStr = parseData(opConfig.format);

    // Let the exporters prevent empty slices from making it through
    if (!outStr || outStr.length === 0 || outStr === opConfig.line_delimiter) {
        return null;
    }

    return compress(opConfig.compression, outStr);
}
