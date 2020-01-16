'use strict';

const { Fetcher } = require('@terascope/job-components');
const fse = require('fs-extra');
const { getChunk } = require('../_lib/chunked-file-reader');
const { decompress } = require('../_lib/compression');

class FileFetcher extends Fetcher {
    constructor(context, opConfig, executionConfig) {
        super(context, opConfig, executionConfig);
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
        const reader = async (offset, length) => {
            const fd = await fse.open(slice.path, 'r');
            try {
                const buf = Buffer.alloc(2 * this.opConfig.size);
                const { bytesRead } = await fse.read(fd, buf, 0, length, offset);
                return decompress(buf.slice(0, bytesRead), this.opConfig.compression);
            } finally {
                fse.close(fd);
            }
        };
        // Passing the slice in as the `metadata`. This will include the path, offset, and length
        return getChunk(reader, slice, this.opConfig, this.logger, slice);
    }
}

module.exports = FileFetcher;
