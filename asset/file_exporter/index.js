'use strict';

const Promise = require('bluebird');
const path = require('path');
const fs = require('fs');
const json2csv = require('json2csv').parse;
const { TSError } = require('@terascope/utils');

Promise.promisifyAll(fs);

function newProcessor(context, opConfig) {
    // This will need to be changed to the worker name since multiple workers on a node would result
    // in write conflicts between workers
    const worker = context.sysconfig._nodeName;
    const filePrefix = opConfig.file_prefix;
    let filePerSlice = opConfig.file_per_slice;
    const filenameBase = path.join(opConfig.path, `${filePrefix}${worker}`);
    let fileNum = 0;

    // Used as a guard against dropping the header mid-file
    let firstSlice = true;

    // `file_per_slice` needs to be forced to `true` if the format is JSON to provide a sensible
    // output
    if (opConfig.format === 'json') {
        filePerSlice = true;
    }

    // Set the options for the parser
    const csvOptions = {};
    // Only need to set `fields` if there is a custom list since the library will, by default,
    // use the record' top-level attributes. This might be a problem if records are missing
    // attirbutes
    if (opConfig.fields.length !== 0) {
        csvOptions.fields = opConfig.fields;
    }

    csvOptions.header = opConfig.include_header;
    csvOptions.eol = opConfig.line_delimiter;

    // Assumes a custom delimiter will be used only if the `csv` output format is chosen
    if (opConfig.format === 'csv') {
        csvOptions.delimiter = opConfig.field_delimiter;
    } else if (opConfig.format === 'tsv') {
        csvOptions.delimiter = '\t';
    }

    // Determines the filname based on the settings
    function getFilename() {
        if (filePerSlice) {
            // Increment the file number tracker by one and use the previous number
            fileNum += 1;
            return `${filenameBase}.${fileNum - 1}`;
        }
        // Make sure header does not show up mid-file if the worker is writing all slices to a
        // single file
        if (!firstSlice) {
            csvOptions.header = false;
        }
        firstSlice = false;
        return filenameBase;
    }

    return (data) => {
        // Converts the slice to a string, formatted based on the configuration options selected;
        function buildOutputString(slice) {
            switch (opConfig.format) {
            case 'csv':
            case 'tsv':
                // null or empty slices will manifest as blank lines in the output file
                if (!slice || !slice.length) return opConfig.line_delimiter;
                return `${json2csv(slice, csvOptions)}${opConfig.line_delimiter}`;
            case 'raw': {
                let outStr = '';
                slice.forEach((record) => {
                    outStr = `${outStr}${record.data}\n`;
                });
                return outStr;
            }
            case 'ldjson': {
                let outStr = '';
                if (opConfig.fields.length > 0) {
                    slice.forEach((record) => {
                        outStr = `${outStr}${JSON.stringify(record, opConfig.fields)}${opConfig.line_delimiter}`;
                    });
                } else {
                    slice.forEach((record) => {
                        outStr = `${outStr}${JSON.stringify(record)}${opConfig.line_delimiter}`;
                    });
                }
                return outStr;
            }
            case 'json': {
                // This case assumes the data is just a single record in the slice's data array. We
                // could just strigify the slice as-is, but feeding the output back into the reader
                // would just nest that array into a record in that slice's array, which probably
                // isn't the desired effect.
                const outStr = `${JSON.stringify(slice)}${opConfig.line_delimiter}`;
                return outStr;
            }
            // Schema validation guards against this
            default:
                throw new Error('Unsupported output format!!');
            }
        }
        const fileName = getFilename();
        return fs.appendFileAsync(fileName, buildOutputString(data))
            .catch(err => Promise.reject(new TSError(err, {
                reason: `Failure to append to file ${fileName}`
            })));
    };
}

function schema() {
    return {
        path: {
            doc: 'Path to the file where the data will be saved to, directory must pre-exist.',
            default: null,
            format: 'required_String'
        },
        file_prefix: {
            doc: 'Optional prefix to prepend to the file name.',
            default: 'export_',
            format: String
        },
        fields: {
            doc: 'List of fields to extract from the incoming records and save to the file. '
            + 'The order here determines the order of columns in the file.',
            default: [],
            format: Array
        },
        field_delimiter: {
            doc: 'Delimiter to use for separating fields in the output file.',
            default: ',',
            format: String
        },
        line_delimiter: {
            doc: 'Delimiter to use for records in the output file.',
            default: '\n',
            format: String
        },
        file_per_slice: {
            doc: 'Determines if a new file is created for each slice.',
            default: false,
            format: Boolean
        },
        include_header: {
            doc: 'Determines whether or not to include a header at the top of the file. '
            + 'The header will consist of the field names.',
            default: false,
            format: Boolean
        },
        format: {
            doc: 'Specifies the output format of the file. Supported formats are csv, tsv, json,'
            + ' and text, where each line of the output file will be a separate record.',
            default: 'ldjson',
            format: ['json', 'ldjson', 'raw', 'csv', 'tsv']
        }
    };
}

module.exports = {
    newProcessor,
    schema
};
