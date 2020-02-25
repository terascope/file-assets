import { Fetcher } from '@terascope/job-components';
import fse from 'fs-extra';
import { FileConfig } from './interfaces';
import { getChunk } from '../__lib/chunked-file-reader';
import { decompress } from '../__lib/compression';

export default class FileFetcher extends Fetcher<FileConfig> {
    _initialized = false;
    _shutdown = false

    async initialize() {
        this._initialized = true;
        return super.initialize();
    }

    async shutdown() {
        this._shutdown = true;
        return super.shutdown();
    }

    async fetch(slice: any) {
        const reader = async (offset: number, length: number) => {
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
