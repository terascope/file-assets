import { TSError } from '@terascope/job-components';
import fse from 'fs-extra';
import { FileSlicer } from './file-slicer';
import { SlicedFileResults, FileSliceConfig, SliceConfig } from '../interfaces';
import { ChunkedFileReader, segmentFile, canReadFile } from '../lib';

export class FileReader extends ChunkedFileReader {
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

    canReadFile(fileName: string): boolean {
        return canReadFile(fileName);
    }

    validatePath(path: string): void {
        try {
            const dirStats = fse.lstatSync(path);

            if (dirStats.isSymbolicLink()) {
                throw new TSError({ reason: `Directory '${path}' cannot be a symlink!` });
            }

            const dirContents = fse.readdirSync(path);

            if (dirContents.length === 0) {
                throw new TSError({ reason: `Directory '${path}' must not be empty!` });
            }
        } catch (err) {
            const error = new TSError(err, { reason: 'Path must be valid!' });
            throw error;
        }
    }

    segmentFile(file: {
        path: string;
        size: number;
    }, config: SliceConfig): SlicedFileResults[] {
        return segmentFile(file, config);
    }

    async makeSlicer(config: FileSliceConfig): Promise<FileSlicer> {
        return new FileSlicer(config, this.logger);
    }
}
