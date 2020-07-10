import fse from 'fs-extra';
import { SlicedFileResults } from '../__lib/interfaces';
import ChunkedReader from '../__lib/chunked-file-reader';

export default class FileReader extends ChunkedReader {
    client = fse;

    async fetch(slice: SlicedFileResults): Promise<string> {
        const { path, length, offset } = slice;
        const fd = await fse.open(path, 'r');

        try {
            const buf = Buffer.alloc(2 * this.config.size);
            const { bytesRead } = await fse.read(fd, buf, 0, length, offset);
            return this.decompress(buf.slice(0, bytesRead));
        } finally {
            fse.close(fd);
        }
    }
}
