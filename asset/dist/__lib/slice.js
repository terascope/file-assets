"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const chunked_file_reader_1 = require("./chunked-file-reader");
function sliceFile(file, config) {
    const slices = [];
    if (config.format === 'json' || config.file_per_slice) {
        slices.push({
            path: file.path,
            offset: 0,
            total: file.size,
            length: file.size
        });
    }
    else {
        chunked_file_reader_1.getOffsets(config.size, file.size, config.line_delimiter).forEach((offset) => {
            offset.path = file.path;
            offset.total = file.size;
            slices.push(offset);
        });
    }
    return slices;
}
exports.sliceFile = sliceFile;
// Batches records in a slice into groups based on the `routingPath` override (if present)
function batchSlice(data, defaultPath) {
    const batches = {};
    batches[defaultPath] = [];
    data.forEach((record) => {
        const override = record.getMetadata('standard:route');
        if (override) {
            if (!batches[override]) {
                batches[override] = [];
            }
            batches[override].push(record);
        }
        else {
            batches[defaultPath].push(record);
        }
    });
    return batches;
}
exports.batchSlice = batchSlice;
//# sourceMappingURL=slice.js.map