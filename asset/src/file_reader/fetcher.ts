import { Fetcher, WorkerContext, ExecutionConfig } from '@terascope/job-components';
import fse from 'fs-extra';
import { FileConfig } from './interfaces';
import { SlicedFileResults } from '../__lib/interfaces';
import { getChunk, FetcherFn } from '../__lib/chunked-file-reader';
import { decompress } from '../__lib/compression';

export default class FileFetcher extends Fetcher<FileConfig> {
    reader: FetcherFn;

    constructor(context: WorkerContext, opConfig: FileConfig, exConfig: ExecutionConfig) {
        super(context, opConfig, exConfig);
        this.reader = this.fileReader.bind(this);
    }

    async fileReader(slice: SlicedFileResults) {
        const { path, length, offset } = slice;
        const fd = await fse.open(path, 'r');

        try {
            const buf = Buffer.alloc(2 * this.opConfig.size);
            const { bytesRead } = await fse.read(fd, buf, 0, length, offset);
            return decompress(buf.slice(0, bytesRead), this.opConfig.compression);
        } finally {
            fse.close(fd);
        }
    }

    async fetch(slice: SlicedFileResults) {
        return getChunk(this.reader, this.opConfig, this.logger, slice);
    }
}
