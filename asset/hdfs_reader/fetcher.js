'use strict';

const {
    Fetcher, getClient
} = require('@terascope/job-components');
const { getChunk } = require('../_lib/chunked-file-reader');
const { decompress } = require('../_lib/compression');

class HDFSFetcher extends Fetcher {
    constructor(context, opConfig, executionConfig) {
        super(context, opConfig, executionConfig);
        this.client = getClient(context, opConfig, 'hdfs_ha');
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
                offset,
                length
            };
            return this.client.openAsync(slice.path, opts);
        };
        // Passing the slice in as the `metadata`. This will include the path, offset, and length
        return getChunk(reader, slice, this.opConfig, this.logger, slice)
            .then((object) => decompress(object.Body, this.opConfig.compression));
    }
}

module.exports = HDFSFetcher;
