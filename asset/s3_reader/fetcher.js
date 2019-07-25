'use strict';

const {
    Fetcher, getClient
} = require('@terascope/job-components');
const { getChunk } = require('@terascope/chunked-file-reader');

class S3Fetcher extends Fetcher {
    constructor(context, opConfig, executionConfig) {
        super(context, opConfig, executionConfig);
        this.client = getClient(context, opConfig, 's3');
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
        // Coerce the field delimiter if the format is `tsv`
        if (this.opConfig.format === 'tsv') {
            this.opConfig.field_delimiter = '\t';
        }
        const reader = (offset, length) => {
            const opts = {
                Bucket: this.opConfig.bucket,
                Key: slice.path,
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
            return this.client.getObject_Async(opts)
                .then(object => object.Body.toString());
        };
        // Passing the slice in as the `metadata`. This will include the path, offset, and length
        return getChunk(reader, slice, this.opConfig, this.logger, slice);
    }
}

module.exports = S3Fetcher;
