import { DataEntity, isTest } from '@terascope/utils';
import { Compressor } from './Compressor';
import { Formatter } from './Formatter';

export interface Chunk {
    /**
     * The ordered index, starts at 0, then incremented per chunk
    */
    readonly index: number;

    /**
     * The chunk of data to published
    */
    readonly data: Buffer;

    /**
     * Indicates whether there are more chunks to be processed
    */
    readonly has_more: boolean;
}

/**
 * Efficiently breaks up a slice into multiple chunks.
 * The behavior will change depending if the whole slice has be
 * serialized at once or not.
*/
export class ChunkGenerator {
    /**
     * Used for determine how big each chunk of a single file should be
    */
    static MAX_CHUNK_SIZE_BYTES = (isTest ? 5 : 100) * 1024 * 1024;

    /*
    * 5MiB - Minimum part size for multipart uploads with Minio
    */
    static MIN_CHUNK_SIZE_BYTES = 1024 * 1024 * 5;

    constructor(
        readonly formatter: Formatter,
        readonly compressor: Compressor,
        readonly slice: (Record<string, unknown>|DataEntity)[]
    ) {}

    async* [Symbol.asyncIterator](): AsyncIterableIterator<Chunk> {
        if (!this.slice.length) return;

        const formattedData = this.formatter.format(this.slice);
        const data = await this.compressor.compress(formattedData);

        const chunkSize = getBytes(ChunkGenerator.MAX_CHUNK_SIZE_BYTES);
        const numChunks = Math.ceil(data.length / chunkSize);

        let offset = 0;
        for (let i = 0; i < numChunks; i++) {
            const chunk = data.subarray(offset, offset + chunkSize);
            yield {
                index: i,
                data: chunk,
                has_more: (i + 1) !== numChunks
            };
            offset += chunk.length;
        }
    }
}

/**
 * Convert MiB to bytes with a hard minimum
*/
function getBytes(bytes: number): number {
    return Math.max(bytes, ChunkGenerator.MIN_CHUNK_SIZE_BYTES);
}
