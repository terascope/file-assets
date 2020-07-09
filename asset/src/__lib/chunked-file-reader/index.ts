import {
    Logger, cloneDeep, DataEntity, isNotNil, AnyObject
} from '@terascope/job-components';
import * as chunkFormatter from './formatters';
import { SlicedFileResults, Offsets } from '../interfaces';
import FileFormatter from '../compression';

export function _averageRecordSize(array: string[]): number {
    return Math.floor(array.reduce((accum, str) => accum + str.length, 0) / array.length);
}

export type FetcherFn = (slice: SlicedFileResults) => Promise<string>

// [{offset, length}] of chunks `size` assuming `delimiter` for a file with `total` size.
export function getOffsets(size: number, total: number, delimiter: string): Offsets[] {
    if (total === 0) {
        return [];
    }

    if (total < size) {
        return [{ length: total, offset: 0 }];
    }

    const fullChunks = Math.floor(total / size);
    const delta = delimiter.length;
    const length = size + delta;
    const chunks = [];

    for (let chunk = 1; chunk < fullChunks; chunk += 1) {
        chunks.push({ length, offset: (chunk * size) - delta });
    }

    // First chunk doesn't need +/- delta.
    chunks.unshift({ offset: 0, length: size });
    // When last chunk is not full chunk size.
    const lastChunk = total % size;

    if (lastChunk > 0) {
        chunks.push({ offset: (fullChunks * size) - delta, length: lastChunk + delta });
    }

    return chunks;
}

export default abstract class ChunkedFileReader extends FileFormatter {
    client: any;
    config: AnyObject;
    logger: Logger;

    constructor(client: AnyObject, config: AnyObject, logger: Logger) {
        super(config.compression);
        this.client = client;
        this.config = config;
        this.logger = logger;
    }

    abstract async fetch(msg: AnyObject): Promise<string>

    // This method will grab the chunk of data specified by the slice plus an
    // extra margin if the slice does not end with the delimiter.
    private async getChunk(
        slice: SlicedFileResults,
    ): Promise<DataEntity<any, any>[]> {
        const delimiter = this.config.line_delimiter;

        let needMargin = false;
        if (slice.length) {
            // Determines whether or not to grab the extra margin.
            if (slice.offset + slice.length !== slice.total) {
                needMargin = true;
            }
        }

        const data = await this.fetch(slice);
        let collectedData = data;

        if (data.endsWith(delimiter)) {
            // Skip the margin if the raw data ends with the delimiter since
            // it will end with a complete record.
            needMargin = false;
        }

        if (needMargin) {
            // Want to minimize reads since will typically be over the
            // network. Using twice the average record size as a heuristic.
            const avgSize = _averageRecordSize(data.split(delimiter));
            const newSlice = cloneDeep(slice);
            newSlice.offset = slice.offset + slice.length;
            newSlice.length = 2 * avgSize;

            collectedData += await this.getMargin(newSlice, delimiter);
        }

        const results = await chunkFormatter[this.config.format](
            collectedData, this.logger, this.config, slice
        );

        if (results) return results.filter(isNotNil) as DataEntity<any, any>[];
        return results;
    }

    private async getMargin(slice: SlicedFileResults, delimiter: string) {
        const { offset, length } = slice;
        let margin = '';
        let currentOffset = offset;

        while (margin.indexOf(delimiter) === -1) {
            // reader clients must return false-y when nothing more to read.
            const newSlice = cloneDeep(slice);
            newSlice.offset = currentOffset;

            const chunk = await this.fetch(newSlice);

            if (!chunk) {
                return margin.split(delimiter)[0];
            }

            margin += chunk;
            currentOffset += length;
        }
        // Don't read too far - next slice will get it.
        return margin.split(delimiter)[0];
    }

    async read(slice: SlicedFileResults): Promise<DataEntity[]> {
        return this.getChunk(slice);
    }
}
