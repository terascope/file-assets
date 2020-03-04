"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const node_gzip_1 = require("node-gzip");
// @ts-ignore
const lz4_1 = require("lz4");
var Compression;
(function (Compression) {
    Compression["none"] = "none";
    Compression["lz4"] = "lz4";
    // eslint-disable-next-line no-shadow
    Compression["gzip"] = "gzip";
})(Compression = exports.Compression || (exports.Compression = {}));
async function compress(data, compression) {
    switch (compression) {
        case 'lz4':
            return lz4_1.encode(data);
        case 'gzip':
            return node_gzip_1.gzip(data);
        case 'none':
            return data;
        default:
            // This shouldn't happen since the config schemas will protect against it
            throw new Error(`Unsupported compression: ${compression}`);
    }
}
exports.compress = compress;
async function decompress(data, compression) {
    switch (compression) {
        case 'lz4':
            return lz4_1.decode(data).toString();
        case 'gzip':
            return node_gzip_1.ungzip(data).then((uncompressed) => uncompressed.toString());
        case 'none':
            return data.toString();
        default:
            // This shouldn't happen since the config schemas will protect against it
            throw new Error(`Unsupported compression: ${compression}`);
    }
}
exports.decompress = decompress;
//# sourceMappingURL=compression.js.map