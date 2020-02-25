"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const job_components_1 = require("@terascope/job-components");
const fs_extra_1 = __importDefault(require("fs-extra"));
const chunked_file_reader_1 = require("../__lib/chunked-file-reader");
const compression_1 = require("../__lib/compression");
class FileFetcher extends job_components_1.Fetcher {
    constructor() {
        super(...arguments);
        this._initialized = false;
        this._shutdown = false;
    }
    async initialize() {
        this._initialized = true;
        return super.initialize();
    }
    async shutdown() {
        this._shutdown = true;
        return super.shutdown();
    }
    async fetch(slice) {
        const reader = async (offset, length) => {
            const fd = await fs_extra_1.default.open(slice.path, 'r');
            try {
                const buf = Buffer.alloc(2 * this.opConfig.size);
                const { bytesRead } = await fs_extra_1.default.read(fd, buf, 0, length, offset);
                return compression_1.decompress(buf.slice(0, bytesRead), this.opConfig.compression);
            }
            finally {
                fs_extra_1.default.close(fd);
            }
        };
        // Passing the slice in as the `metadata`. This will include the path, offset, and length
        return chunked_file_reader_1.getChunk(reader, slice, this.opConfig, this.logger, slice);
    }
}
exports.default = FileFetcher;
//# sourceMappingURL=fetcher.js.map