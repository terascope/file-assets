'use strict';

const { getOffsets } = require('./chunked-file-reader');

function sliceFile(file, opConfig) {
    const slices = [];
    if (opConfig.format === 'json' || opConfig.file_per_slice) {
        slices.push({
            path: file.path,
            offset: 0,
            length: file.size
        });
    } else {
        getOffsets(
            opConfig.size,
            file.size,
            opConfig.line_delimiter
        ).forEach((offset) => {
            offset.path = file.path;
            offset.total = file.size;
            slices.push(offset);
        });
    }
    return slices;
}

// Batches records in a slice into groups based on the `routingPath` override (if present)
function batchSlice(slice, defaultPath) {
    const batches = {};
    batches[defaultPath] = [];
    slice.forEach((record) => {
        const override = record.getMetadata('file:partition');
        if (override) {
            if (!batches[override]) {
                batches[override] = [];
            }
            batches[override].push(record);
        } else {
            batches[defaultPath].push(record);
        }
    });
    return batches;
}

module.exports = {
    sliceFile,
    batchSlice
};
