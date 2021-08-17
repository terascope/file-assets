import {
    DataEntity, EventLoop, isTest
} from '@terascope/utils';
import { Compressor } from './Compressor';
import { Formatter } from './Formatter';
import { Format, Compression } from '../interfaces';

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

const MiB = 1024 * 1024;

/**
 * Efficiently breaks up a slice into multiple chunks.
 * The behavior will change depending if the whole slice has be
 * serialized at once or not.
 *
 * The chunk size is not a guaranteed since we check the length
*/
export class ChunkGenerator {
    /**
     * Used for determine how big each chunk of a single file should be
    */
    static MAX_CHUNK_SIZE_BYTES = (isTest ? 5 : 100) * MiB;

    /**
     * Used for determine how much the chunk must overflow byte to
     * defer the data to the next chunk
    */
    static MAX_CHUNK_OVERFLOW_BYTES = (isTest ? 0 : 1) * MiB;

    /*
    * 5MiB - Minimum part size for multipart uploads with Minio
    */
    static MIN_CHUNK_SIZE_BYTES = 5 * MiB;

    constructor(
        readonly formatter: Formatter,
        readonly compressor: Compressor,
        readonly slice: (Record<string, unknown>|DataEntity)[]
    ) {}

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

    async* [Symbol.asyncIterator](): AsyncIterableIterator<Chunk> {
        if (!this.slice.length) return;

        if (this.isRowOptimized()) {
            yield* this._chunkByRow();
        } else {
            yield* this._chunkAll();
        }
    }

    private async* _chunkByRow(): AsyncIterableIterator<Chunk> {
        const chunkSize = getBytes(ChunkGenerator.MAX_CHUNK_SIZE_BYTES);
        const maxOverflowBytes = ChunkGenerator.MAX_CHUNK_OVERFLOW_BYTES;
        let index = 0;

        /**
         * This will use the length of the string
         * to determine the rough number of bytes
        */
        let chunkStr = '';
        /**
         * this is used to count the number of
         * small data pieces so we can break out the event loop
        */
        let tooSmallOfDataCount = 0;

        let chunk: Chunk|undefined;
        for (const [str, has_more] of this.formatter.formatIterator(this.slice)) {
            chunk = undefined;

            chunkStr += str;

            /**
             * Since a row may push the chunk size over the limit,
             * the overflow from the current row buffer needs to
             * be deferred until the next iteration
            */
            const estimatedOverflowBytes = chunkStr.length - chunkSize;
            if (estimatedOverflowBytes > maxOverflowBytes) {
                const combinedBuffer = Buffer.from(chunkStr);
                chunkStr = combinedBuffer.toString('utf-8', chunkSize);

                chunk = {
                    index,
                    has_more,
                    data: combinedBuffer.slice(0, chunkSize),
                };

                index++;
            } else if (estimatedOverflowBytes >= 0) {
                const combinedBuffer = Buffer.from(chunkStr);
                chunkStr = '';

                chunk = {
                    index,
                    has_more,
                    data: combinedBuffer,
                };

                index++;
            }

            if (chunk) {
                tooSmallOfDataCount = 0;
                yield chunk;
            } else {
                tooSmallOfDataCount++;
            }

            // this will ensure we don't block the event loop
            // for too long blocking requests from going out
            if (tooSmallOfDataCount % 1000 === 999) {
                await EventLoop.wait();
            }
        }

        if (chunkStr.length) {
            const uploadBuffer = Buffer.from(chunkStr);
            chunkStr = '';

            chunk = {
                index,
                has_more: false,
                data: uploadBuffer,
            };

            yield chunk;
        }
    }

    private async* _chunkAll(): AsyncIterableIterator<Chunk> {
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
