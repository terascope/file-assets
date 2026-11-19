import { isTest } from '@terascope/core-utils';
import { Compressor } from './Compressor.js';
import { Formatter } from './Formatter.js';
import {
    Format, Compression, SendRecords, SendBytes
} from '../interfaces.js';

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

export const MiB = 1024 * 1024;

/** 100 MiB - Used for determine how big each chunk of a single file should be */
export const MAX_CHUNK_SIZE_BYTES = (isTest ? 6 : 100) * MiB;

/** 5MiB - Minimum part size for multipart uploads with S3 */
export const MIN_CHUNK_SIZE_BYTES = 5 * MiB;

/**
 * 8MiB - How big each chunk of a file's bytes sent as a stream (sendStream)
 * is by default, the AWS CLI's multipart chunk size. A stream holds about
 * (concurrency + 1) chunks, bigger chunks only cost memory, and S3's 10,000
 * parts make a file up to ~78GiB
*/
export const STREAM_CHUNK_SIZE_BYTES = 8 * MiB;

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
    readonly chunkSize: number;

    constructor(
        readonly formatter: Formatter,
        readonly compressor: Compressor,
        readonly slice: SendRecords | SendBytes,
        limits?: { maxBytes?: number; minBytes?: number },
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
        if (isSendBytes(this.slice)) {
            return this._chunkBytes(this.slice);
        }
        if (Array.isArray(this.slice) && this.slice.length === 0) {
            return this._emptyIterator();
        }
        if (this.isRowOptimized()) {
            return this._chunkByRow(this.slice);
        }
        return this._chunkAll(this.slice);
    }

    /**
     * Cuts a file's bytes, already formatted, into chunks of chunkSize as
     * they arrive, the last one smaller. Only one chunk and the bytes of the
     * next are held, never the file: a full chunk is yielded as soon as more
     * bytes arrive, which is when has_more is known
    */
    private async* _chunkBytes(bytes: SendBytes): AsyncIterableIterator<Chunk> {
        if (this.compressor.type !== Compression.none) {
            throw new Error('Sending bytes does not support compression, compress them before sending');
        }

        let index = 0;
        let ready: Buffer | undefined;
        let pending: Buffer[] = [];
        let pendingSize = 0;

        for await (const buffer of joinStrings(bytes)) {
            if (ready) {
                yield { index: index++, data: ready, has_more: true };
                ready = undefined;
            }

            pending.push(buffer);
            pendingSize += buffer.length;

            while (pendingSize >= this.chunkSize) {
                if (ready) yield { index: index++, data: ready, has_more: true };

                const all = pending.length === 1 ? pending[0] : Buffer.concat(pending, pendingSize);
                ready = all.subarray(0, this.chunkSize);
                const rest = all.subarray(this.chunkSize);
                pending = rest.length ? [rest] : [];
                pendingSize = rest.length;
            }
        }

        if (ready) yield { index: index++, data: ready, has_more: pendingSize > 0 };
        if (pendingSize) {
            yield { index, data: Buffer.concat(pending, pendingSize), has_more: false };
        }
    }

    private async* _chunkByRow(records: SendRecords): AsyncIterableIterator<Chunk> {
        let index = 0;

        /**
         * This will use the length of the string
         * to determine the rough number of bytes
        */
        let chunkStr = '';

        let chunk: Chunk | undefined;
        for (const [str, has_more] of this.formatter.formatIterator(records)) {
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

    private async* _chunkAll(records: SendRecords): AsyncIterableIterator<Chunk> {
        const formattedData = this.formatter.format(records);
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

/**
 * Strings are joined up to this many characters before they become bytes,
 * one Buffer per small string (like a line of ldjson) is ~1/3 slower
*/
const JOIN_STRINGS_CHARS = 64 * 1024;

/**
 * The bytes as non-empty Buffers, in order, small strings joined
*/
async function* joinStrings(bytes: SendBytes): AsyncIterableIterator<Buffer> {
    let strings: string[] = [];
    let chars = 0;

    for await (const piece of bytes) {
        if (typeof piece === 'string') {
            if (!piece.length) continue;
            strings.push(piece);
            chars += piece.length;
            if (chars < JOIN_STRINGS_CHARS) continue;
        } else if (!piece.length) {
            continue;
        }

        if (strings.length) {
            yield Buffer.from(strings.join(''));
            strings = [];
            chars = 0;
        }
        if (typeof piece !== 'string') yield piece;
    }

    if (strings.length) yield Buffer.from(strings.join(''));
}

function isSendBytes(input: SendRecords | SendBytes): input is SendBytes {
    return input != null && typeof (input as SendBytes)[Symbol.asyncIterator] === 'function';
}
