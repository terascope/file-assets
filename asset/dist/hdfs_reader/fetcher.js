"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const job_components_1 = require("@terascope/job-components");
const chunked_file_reader_1 = require("../__lib/chunked-file-reader");
const compression_1 = require("../__lib/compression");
class HDFSFetcher extends job_components_1.Fetcher {
    async initialize() {
        await super.initialize();
        this.client = job_components_1.getClient(this.context, this.opConfig, 'hdfs_ha');
    }
    // TODO: decompress returns a string, but it should be a dataentity
    // @ts-ignore
    async fetch(slice) {
        const reader = async (offset, length) => {
            const opts = {
                offset,
                length
            };
            return this.client.openAsync(slice.path, opts);
        };
        // Passing the slice in as the `metadata`. This will include the path, offset, and length
        const results = await chunked_file_reader_1.getChunk(reader, slice, this.opConfig, this.logger, slice);
        return compression_1.decompress(results.Body, this.opConfig.compression);
    }
}
exports.default = HDFSFetcher;
//# sourceMappingURL=fetcher.js.map