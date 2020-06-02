import json2csv, { parse } from 'json2csv';
import { DataEntity, TSError } from '@terascope/job-components';
import {
    CSVOptions, CSVConfig, ParseOptions, Format
} from './interfaces';
import { compress } from './compression';

export function makeCsvOptions(config: CSVConfig): CSVOptions {
    const csvOptions: CSVOptions = {};

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

export async function parseForFile(
    slice: DataEntity[] | null | undefined, opConfig: ParseOptions, csvOptions: CSVOptions
): Promise<any|null> {
    // null or empty slices get an empty output and will get filtered out below
    if (!slice || !slice.length) return null;
    // Build the output string to dump to the object
    // TODO externalize this into a ./lib/ for use with the `file_exporter`
    // let outStr = '';
    const fn = getFormatFn(opConfig.format);
    const outStr = fn(slice, opConfig, csvOptions);
    // Let the exporters prevent empty slices from making it through
    if (!outStr || outStr.length === 0 || outStr === opConfig.line_delimiter) {
        return null;
    }

    return compress(outStr, opConfig.compression);
}
