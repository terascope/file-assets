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
        let index = 0;

        const buffers: Buffer[] = [];
        let buffersLength = 0;

        let chunk: Chunk|undefined;
        for (const [formatted, has_more] of this.formatter.formatIterator(this.slice)) {
            chunk = undefined;

            const buf = Buffer.from(formatted, 'utf8');
            buffers.push(buf);
            buffersLength += buf.length;

            /**
             * Since a row may push the chunk size over the limit,
             * the overflow from the current row buffer needs to
             * be deferred until the next iteration
            */
            const overflowBytes = buffersLength - chunkSize;
            if (overflowBytes > 0) {
                const combinedBuffer = Buffer.concat(
                    buffers,
                    buffersLength
                );
                buffersLength = 0;
                buffers.length = 0;

                const overflowBuf = Buffer.alloc(overflowBytes);
                combinedBuffer.copy(overflowBuf, 0, chunkSize);

                const uploadBuf = Buffer.alloc(chunkSize, combinedBuffer);
                chunk = {
                    index,
                    has_more,
                    data: uploadBuf,
                };

                index++;
                buffersLength += overflowBuf.length;
                buffers.push(overflowBuf);
            } else if (overflowBytes === 0) {
                const combinedBuffer = Buffer.concat(
                    buffers,
                    buffersLength
                );
                buffersLength = 0;
                buffers.length = 0;

                chunk = {
                    index,
                    has_more,
                    data: combinedBuffer,
                };

                index++;
            }

            if (chunk) {
                yield chunk;
                await EventLoop.wait();
            }
        }

        if (buffers.length) {
            const combinedBuffer = Buffer.concat(
                buffers,
                buffersLength
            );
            buffersLength = 0;
            buffers.length = 0;

            chunk = {
                index,
                has_more: false,
                data: combinedBuffer,
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
