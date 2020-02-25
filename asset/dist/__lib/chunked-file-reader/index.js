"use strict";
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) result[k] = mod[k];
    result["default"] = mod;
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
const chunkFormatter = __importStar(require("./formatters"));
function _averageRecordSize(array) {
    return Math.floor(array.reduce((accum, str) => accum + str.length, 0) / array.length);
}
exports._averageRecordSize = _averageRecordSize;
// [{offset, length}] of chunks `size` assuming `delimiter` for a file with `total` size.
function getOffsets(size, total, delimiter) {
    if (total === 0) {
        return [];
    }
    if (total < size) {
        return [{ length: total, offset: 0 }];
    }
    const fullChunks = Math.floor(total / size);
    const delta = delimiter.length;
    const length = size + delta;
    const chunks = [];
    for (let chunk = 1; chunk < fullChunks; chunk += 1) {
        chunks.push({ length, offset: (chunk * size) - delta });
    }
    // First chunk doesn't need +/- delta.
    chunks.unshift({ offset: 0, length: size });
    // When last chunk is not full chunk size.
    const lastChunk = total % size;
    if (lastChunk > 0) {
        chunks.push({ offset: (fullChunks * size) - delta, length: lastChunk + delta });
    }
    return chunks;
}
exports.getOffsets = getOffsets;
async function getMargin(readerClient, delimiter, offset, length) {
    let margin = '';
    let currentOffset = offset;
    while (margin.indexOf(delimiter) === -1) {
        // reader clients must return false-y when nothing more to read.
        const chunk = await readerClient(currentOffset, length);
        if (!chunk) {
            return margin.split(delimiter)[0];
        }
        margin += chunk;
        currentOffset += length;
    }
    // Don't read too far - next slice will get it.
    return margin.split(delimiter)[0];
}
// This function will grab the chunk of data specified by the slice plus an
// extra margin if the slice does not end with the delimiter.
async function getChunk(readerClient, slice, opConfig, logger, metadata) {
    const delimiter = opConfig.line_delimiter;
    let needMargin = false;
    if (slice.length) {
        // Determines whether or not to grab the extra margin.
        if (slice.offset + slice.length !== slice.total) {
            needMargin = true;
        }
    }
    const data = await readerClient(slice.offset, slice.length);
    let collectedData = data;
    if (data.endsWith(delimiter)) {
        // Skip the margin if the raw data ends with the delimiter since
        // it will end with a complete record.
        needMargin = false;
    }
    if (needMargin) {
        // Want to minimize reads since will typically be over the
        // network. Using twice the average record size as a heuristic.
        const avgSize = _averageRecordSize(data.split(delimiter));
        const offset = slice.offset + slice.length;
        const length = 2 * avgSize;
        collectedData += await getMargin(readerClient, delimiter, offset, length);
    }
    const results = await chunkFormatter[opConfig.format](collectedData, logger, opConfig, metadata, slice);
    // TODO: what is this filter here for?
    return results.filter((record) => record);
}
exports.getChunk = getChunk;
//# sourceMappingURL=index.js.map