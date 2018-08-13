'use strict';

const Promise = require('bluebird');
const fs = require('fs');
const json2csv = require('json2csv').parse;

Promise.promisifyAll(fs);

function newProcessor(context, opConfig) {
    // This will need to be changed to the worker name since multiple workers on a node would result
    // in write conflicts between workers
    const worker = context.sysconfig._nodeName;
    const filePrefix = opConfig.file_prefix;
    const filePerSlice = opConfig.file_per_slice;

    // Set the options for the parser
    const csvOptions = {};
    // Only need to set `fields` if there is a custom list since the library will, by default,
    // use the record' top-level attributes. This might be a problem if records are missing
    // attirbutes
    if (opConfig.fields.length !== 0) {
        csvOptions.fields = opConfig.fields;
    }

    csvOptions.header = opConfig.include_header;

    // Assumes a custom delimiter will be used only if the `csv` output format is chosen
    if (opConfig.format === 'csv') {
        csvOptions.delimiter = opConfig.delimiter;
    } else if (opConfig.format === 'tsv') {
        csvOptions.delimiter = '\t';
    }

    // Determines the filname based on the settings
    function getFilename(options) {
        const filenameBase = `${filePrefix}_${worker}`;
        // Get the files in the target directly and filter out the ones that contain the worker
        // name
        return fs.readdirAsync(opConfig.path)
            .then((files) => {
                const targetFiles = files.filter(file => file.indexOf(worker) > -1);

                if (filePerSlice) {
                    // In this case, the worker is writing to the first file
                    if (targetFiles.length === 0) {
                        return `${opConfig.path}/${filenameBase}.0`;
                    }
                    // Otherwise, the worker needs to figure out which file suffix is next
                    let biggestNum = 0;
                    targetFiles.forEach((file) => {
                        const curNum = file.split('.').reverse()[0];
                        if (curNum > biggestNum) {
                            biggestNum = curNum;
                        }
                    });
                    // Increment the file's number by one
                    return `${opConfig.path}/${filenameBase}.${biggestNum * 1 + 1}`;
                }
                // Check if the file already exists to make sure the header won't show up mid-file
                if (targetFiles.length > 0) {
                    options.header = false;
                }
                return `${opConfig.path}/${filenameBase}`;
            })
            .catch((err) => {
                throw new Error(err);
            });
    }

    return (data) => {
        // Converts the slice to a string, formatted based on the configuration options selected;
        function buildOutputString(slice) {
            switch (opConfig.format) {
            case 'csv':
                return `${json2csv(slice, csvOptions)}\n`;
            case 'tsv':
                return `${json2csv(slice, csvOptions)}\n`;
            case 'text': {
                let outStr = '';
                slice.forEach((record) => {
                    outStr = `${outStr}${record}\n`;
                });
                return outStr;
            }
            case 'json': {
                let outStr = '';
                slice.forEach((record) => {
                    outStr = `${outStr}${JSON.stringify(record)}\n`;
                });
                return outStr;
            }
            default:
                throw new Error('Unsupported output format!!');
            }
        }

        return getFilename(csvOptions)
            .then(filename => fs.appendFileAsync(filename, buildOutputString(data)))
            .catch((err) => {
                throw new Error(err);
            });
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
            default: 'export',
            format: String
        },
        fields: {
            doc: 'List of fields to extract from the incoming records and save to the file. '
            + 'The order here determines the order of columns in the file.',
            default: [],
            format: Array
        },
        delimiter: {
            doc: 'Delimiter to use in the output file.',
            default: ',',
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
            default: 'json',
            format: function check(val) {
                const formats = ['json', 'text', 'csv', 'tsv'];
                if (formats.indexOf(val) === -1) {
                    throw new Error(
                        `Format must be one of the following supported values: ${formats}`
                    );
                }
            }
        }
    };
}

module.exports = {
    newProcessor,
    schema
};
