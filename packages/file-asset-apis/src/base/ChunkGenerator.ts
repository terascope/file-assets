import { isTest } from '@terascope/utils';
import { Compressor } from './Compressor.js';
import { Formatter } from './Formatter.js';
import { Format, Compression, SendRecords } from '../interfaces.js';

export interface Chunk {
    /**
     * The ordered index, starts at 0, then incremented per chunk
    */
    readonly index: number;

    /**
     * The chunk of data to published
    */
    readonly data: Buffer | string;

    /**
     * Indicates whether there are more chunks to be processed
    */
    readonly has_more: boolean;
}

const MiB = 1024 * 1024;

/** 100 MiB - Used for determine how big each chunk of a single file should be */
export const MAX_CHUNK_SIZE_BYTES = (isTest ? 5 : 100) * MiB;

/** 5MiB - Minimum part size for multipart uploads with Minio */
export const MIN_CHUNK_SIZE_BYTES = 5 * MiB;

/**
 * Efficiently breaks up a slice into multiple chunks.
 * The behavior will change depending if the whole slice has be
 * serialized at once or not.
 *
 * The chunk size is not a guaranteed since we check the length
*/
export class ChunkGenerator {
    /**
     * how big each chunk of a single file should be
     */
    readonly chunkSize = 5 * MiB;

    constructor(
        readonly formatter: Formatter,
        readonly compressor: Compressor,
        readonly slice: SendRecords,
        limits?: { maxBytes?: number; minBytes?: number }
    ) {
        const max = limits?.maxBytes || MAX_CHUNK_SIZE_BYTES;
        const min = limits?.minBytes || MIN_CHUNK_SIZE_BYTES;
        this.chunkSize = getBytes(max, min);
    }

    /**
     * If the format is ldjson and compression is not on,
     * the whole array of records won't have to be serialized
     * at once and can be chunked per record to create the appropriate
     * chunk size.
    */
    isRowOptimized(): boolean {
        return this.formatter.type === Format.ldjson
            && this.compressor.type === Compression.none;
    }

    [Symbol.asyncIterator](): AsyncIterableIterator<Chunk> {
        if (Array.isArray(this.slice) && this.slice.length === 0) {
            return this._emptyIterator();
        }
        if (this.isRowOptimized()) {
            return this._chunkByRow();
        }
        return this._chunkAll();
    }

    private async* _chunkByRow(): AsyncIterableIterator<Chunk> {
        let index = 0;

        /**
         * This will use the length of the string
         * to determine the rough number of bytes
        */
        let chunkStr = '';

        let chunk: Chunk | undefined;
        for (const [str, has_more] of this.formatter.formatIterator(this.slice)) {
            chunk = undefined;

            chunkStr += str;

            /**
             * Since a row may push the chunk size over the limit,
             * the overflow from the current row buffer needs to
             * be deferred until the next iteration
            */
            const estimatedOverflowBytes = chunkStr.length - this.chunkSize;
            if (estimatedOverflowBytes >= this.chunkSize) {
                chunk = {
                    index,
                    has_more,
                    data: chunkStr.slice(0, this.chunkSize),
                };

                chunkStr = chunkStr.slice(this.chunkSize, chunkStr.length);
                index++;
            } else if (estimatedOverflowBytes >= 0) {
                chunk = {
                    index,
                    has_more,
                    data: chunkStr,
                };

                chunkStr = '';
                index++;
            }

            if (chunk) {
                yield chunk;
            }
        }

        if (chunkStr.length) {
            chunk = {
                index,
                has_more: false,
                data: chunkStr,
            };

            chunkStr = '';
            yield chunk;
        }
    }

    private async* _chunkAll(): AsyncIterableIterator<Chunk> {
        const formattedData = this.formatter.format(this.slice);
        const data = await this.compressor.compress(formattedData);

        const numChunks = Math.ceil(data.length / this.chunkSize);

        let offset = 0;
        for (let i = 0; i < numChunks; i++) {
            const chunk = data.subarray(offset, offset + this.chunkSize);
            yield {
                index: i,
                data: chunk,
                has_more: (i + 1) !== numChunks
            };
            offset += chunk.length;
        }
    }

    private async* _emptyIterator(): AsyncIterableIterator<Chunk> {}
}

/**
 * Get bytes with a hard minimum
*/
function getBytes(max: number, min: number): number {
    return Math.max(max, min);
}
