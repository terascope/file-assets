import fse from 'fs-extra';
import { Logger } from '@terascope/utils';
import { FileSlice, ReaderConfig } from '../interfaces';
import { ChunkedFileReader } from '../base';

export class FileFetcher extends ChunkedFileReader {
    client = fse;
    protected readonly size: number;

    constructor(config: ReaderConfig, logger: Logger) {
        super(config, logger);
        const { size } = config;
        this.size = size;
    }

    /**
     * low level api that fetches the unprocessed contents of the file, please use the "read" method
     * for correct file and data parsing
     * @example
     *      const slice = {
     *          offset: 0,
     *          length: 1000,
     *          path: 'some/file.txt',
     *          total: 1000
     *      };
     *      const results = await fileReader.fetch(slice);
     *      results === 'the unprocessed contents of the file here'
    */
    protected async fetch(slice: FileSlice): Promise<string> {
        const { path, length, offset } = slice;
        const fd = await fse.open(path, 'r');

        try {
            const buf = Buffer.alloc(2 * this.size);
            const { bytesRead } = await fse.read(fd, buf, 0, length, offset);
            return this.compressor.decompress(buf.slice(0, bytesRead));
        } finally {
            fse.close(fd);
        }
    }
}
