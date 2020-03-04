"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const json2csv_1 = require("json2csv");
const utils_1 = require("@terascope/utils");
const compression_1 = require("./compression");
var Format;
(function (Format) {
    Format["json"] = "json";
    Format["ldjson"] = "ldjson";
    Format["raw"] = "raw";
    Format["tsv"] = "tsv";
    Format["csv"] = "csv";
})(Format = exports.Format || (exports.Format = {}));
function makeCsvOptions(config) {
    const csvOptions = {};
    if (config.fields.length !== 0) {
        csvOptions.fields = config.fields;
    }
    else {
        csvOptions.fields = undefined;
    }
    csvOptions.header = config.include_header;
    csvOptions.eol = config.line_delimiter;
    // Assumes a custom delimiter will be used only if the `csv` output format is chosen
    if (config.format === 'csv') {
        csvOptions.delimiter = config.field_delimiter;
    }
    else if (config.format === 'tsv') {
        csvOptions.delimiter = '\t';
    }
    return csvOptions;
}
exports.makeCsvOptions = makeCsvOptions;
function csvFunction(slice, opConfig, csvOptions) {
    return `${json2csv_1.parse(slice, csvOptions)}${opConfig.line_delimiter}`;
}
const formatsFns = {
    csv: csvFunction,
    tsv: csvFunction,
    raw(slice, opConfig) {
        return `${slice.map((record) => record.data).join(opConfig.line_delimiter)}${opConfig.line_delimiter}`;
    },
    ldjson(slice, opConfig) {
        return `${slice.map((record) => JSON.stringify(record, (opConfig.fields.length > 0) ? opConfig.fields : undefined)).join(opConfig.line_delimiter)}${opConfig.line_delimiter}`;
    },
    json(slice, opConfig) {
        return `${JSON.stringify(slice)}${opConfig.line_delimiter}`;
    }
};
function getFormatFn(format) {
    const fn = formatsFns[format];
    if (fn == null)
        throw new utils_1.TSError(`Unsupported output format "${format}"`);
    return fn;
}
async function parseForFile(slice, opConfig, csvOptions) {
    // null or empty slices get an empty output and will get filtered out below
    if (!slice || !slice.length)
        return null;
    // Build the output string to dump to the object
    // TODO externalize this into a ./lib/ for use with the `file_exporter`
    // let outStr = '';
    const fn = getFormatFn(opConfig.format);
    const outStr = fn(slice, opConfig, csvOptions);
    // Let the exporters prevent empty slices from making it through
    if (!outStr || outStr.length === 0 || outStr === opConfig.line_delimiter) {
        return null;
    }
    return compression_1.compress(outStr, opConfig.compression);
}
exports.parseForFile = parseForFile;
//# sourceMappingURL=parser.js.map