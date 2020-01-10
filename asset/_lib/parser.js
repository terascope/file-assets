'use strict';

const json2csv = require('json2csv').parse;
const { compress } = require('./compression');

const csv = (slice, opConfig, csvOptions) => `${json2csv(slice, csvOptions)}${opConfig.line_delimiter}`;
const raw = (slice, opConfig) => `${slice.map((record) => record.data).join(opConfig.line_delimiter)}${opConfig.line_delimiter}`;
const ldjson = (slice, opConfig) => `${slice.map(
    (record) => JSON.stringify(record, (opConfig.fields.length > 0) ? opConfig.fields : undefined)
).join(opConfig.line_delimiter)}${opConfig.line_delimiter}`;
const json = (slice, opConfig) => `${JSON.stringify(slice)}${opConfig.line_delimiter}`;

async function parseForFile(slice, opConfig, csvOptions) {
    // null or empty slices get an empty output and will get filtered out below
    if (!slice || !slice.length) return null;
    // Build the output string to dump to the object
    // TODO externalize this into a ./lib/ for use with the `file_exporter`
    // let outStr = '';

    const parseData = (format) => {
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

module.exports = {
    parseForFile
};
