'use strict';

const json2csv = require('json2csv').parse;
const { stringify } = require('./stringify');
const { compress } = require('./compression');

async function parseForFile(slice, opConfig, csvOptions) {
    // null or empty slices get an empty output and will get filtered out below
    if (!slice || !slice.length) return null;
    // Build the output string to dump to the object
    // TODO externalize this into a ./lib/ for use with the `file_exporter`
    // let outStr = '';

    const parseData = (format) => {
        const csv = () => `${json2csv(slice, csvOptions)}${opConfig.line_delimiter}`;
        const raw = () => {
            let outStr;
            slice.forEach((record) => {
                outStr = `${outStr}${record.data}${opConfig.line_delimiter}`;
            });
            return outStr;
        };
        const ldjson = () => {
            let outStr;
            if (opConfig.fields.length > 0) {
                slice.forEach((record) => {
                    outStr = `${outStr}${stringify(record, opConfig.fields)}${opConfig.line_delimiter}`;
                });
            } else {
                slice.forEach((record) => {
                    outStr = `${outStr}${JSON.stringify(record)}${opConfig.line_delimiter}`;
                });
            }
            return outStr;
        };
        const json = () => `${JSON.stringify(slice)}${opConfig.line_delimiter}`;
        const formats = {
            csv,
            tsv: csv,
            raw,
            ldjson,
            json,
            default: new Error(`Unsupported output format "${opConfig.format}"`)
        };
        return (formats[format] || formats.default);
    };
    const outStr = parseData(opConfig.format);

    if (outStr instanceof Error) throw outStr;

    // Let the exporters prevent empty slices from making it through
    if (!outStr || outStr.length === 0 || outStr === opConfig.line_delimiter) {
        return null;
    }

    return compress(opConfig.compression, outStr);
}

module.exports = {
    parseForFile
};
