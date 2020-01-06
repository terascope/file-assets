'use strict';

const json2csv = require('json2csv').parse;
const { stringify } = require('./stringify');
const { compress } = require('./compression');

async function parseForFile(slice, opConfig, csvOptions) {
    // Build the output string to dump to the object
    // TODO externalize this into a ./lib/ for use with the `file_exporter`
    let outStr = '';
    switch (opConfig.format) {
    case 'csv':
    case 'tsv':
        // null or empty slices get an empty output and will get filtered out below
        if (!slice || !slice.length) break;
        else outStr = `${json2csv(slice, csvOptions)}${opConfig.line_delimiter}`;
        break;
    case 'raw': {
        slice.forEach((record) => {
            outStr = `${outStr}${record.data}${opConfig.line_delimiter}`;
        });
        break;
    }
    case 'ldjson': {
        if (opConfig.fields.length > 0) {
            slice.forEach((record) => {
                outStr = `${outStr}${stringify(record, opConfig.fields)}${opConfig.line_delimiter}`;
            });
        } else {
            slice.forEach((record) => {
                outStr = `${outStr}${JSON.stringify(record)}${opConfig.line_delimiter}`;
            });
        }
        break;
    }
    case 'json': {
        // This case assumes the data is just a single record in the slice's data array. We
        // could just strigify the slice as-is, but feeding the output back into the reader
        // would just nest that array into a record in that slice's array, which probably
        // isn't the desired effect.
        outStr = `${JSON.stringify(slice)}${opConfig.line_delimiter}`;
        break;
    }
    // Schema validation guards against this
    default:
        throw new Error(`Unsupported output format "${opConfig.format}"`);
    }

    // Let the exporters prevent empty slices from making it through
    if (outStr.length === 0) {
        return null;
    }

    return compress(opConfig.compression, outStr);
}

module.exports = {
    parseForFile
};
