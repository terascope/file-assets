"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const job_components_1 = require("@terascope/job-components");
const path_1 = __importDefault(require("path"));
const chunked_file_reader_1 = require("../__lib/chunked-file-reader");
const compression_1 = require("../__lib/compression");
const fileName_1 = require("../__lib/fileName");
class S3Fetcher extends job_components_1.Fetcher {
    constructor(context, opConfig, exeConfig) {
        super(context, opConfig, exeConfig);
        this.client = job_components_1.getClient(context, opConfig, 's3');
        const { bucket, prefix } = fileName_1.parsePath(opConfig.path);
        this.bucket = bucket;
        this.prefix = prefix;
    }
    async fetch(slice) {
        const { compression } = this.opConfig;
        // Coerce the field delimiter if the format is `tsv`
        const reader = async (offset, length) => {
            const opts = {
                Bucket: this.bucket,
                // TODO: figure out way to not shaeow slice.path here
                Key: path_1.default.join(this.prefix, path_1.default.basename(slice.path)),
                // We need to subtract 1 from the range in order to avoid collecting an extra byte.
                // i.e. Requesting only the first byte of a file has a `length` of `1`, but the
                //   request would be for `bytes=0-0`
                Range: `bytes=${offset}-${offset + length - 1}`
            };
            /* The object returned looks something like this:
             * {
             *   AcceptRanges: 'bytes',
             *   LastModified: 2019-07-19T22:27:11.000Z,
             *   ContentLength: 51,
             *   ETag: '"xxxx"',
             *   ContentRange: 'bytes 44-94/98',
             *   ContentType: 'text/csv',
             *   Metadata: {
             *     's3cmd-attrs': '...'
             *   },
             *   Body: <Buffer FF FF ... >
             * }
             */
            const results = await this.client.getObject_Async(opts);
            return compression_1.decompress(results.Body, compression);
        };
        // Passing the slice in as the `metadata`. This will include the path, offset, and length
        return chunked_file_reader_1.getChunk(reader, slice, this.opConfig, this.logger, slice);
    }
}
exports.default = S3Fetcher;
//# sourceMappingURL=fetcher.js.map