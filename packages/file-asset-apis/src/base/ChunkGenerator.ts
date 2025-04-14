import { isTest } from '@terascope/utils';
import { DataFrame, SerializeOptions } from '@terascope/data-mate';
import { Compressor } from './Compressor.js';
import { Formatter } from './Formatter.js';
import { Format, Compression, SendRecords } from '../interfaces.js';
import { estimateRecordsPerUpload } from './dataframe-utils.js';

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
export const MAX_CHUNK_SIZE_BYTES = (isTest ? 5 : 5) * MiB;

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
    // TODO probably pass the serialized fram instead
    readonly serializeOptions: SerializeOptions | undefined;

    constructor(
        readonly formatter: Formatter,
        readonly compressor: Compressor,
        readonly slice: SendRecords,
        limits?: { maxBytes?: number; minBytes?: number },
        // TODO probably pass the serialized fram instead
        serializeOptions?: SerializeOptions
    ) {
        const max = limits?.maxBytes || MAX_CHUNK_SIZE_BYTES;
        const min = limits?.minBytes || MIN_CHUNK_SIZE_BYTES;
        this.chunkSize = getBytes(max, min);
        // TODO probably pass the serialized fram instead
        this.serializeOptions = serializeOptions;
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
        if (this.slice instanceof DataFrame) {
            return this._chunkFrame();
        }
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

    private async* _chunkFrame(): AsyncIterableIterator<Chunk> {
        if (!(this.slice instanceof DataFrame)) throw new Error('Invalid call to chunk data frame');

        // TODO format, compression, tests - (if we decide to go with it)
        if (this.compressor.type !== Compression.none) throw new Error('Data frame compression is not yet supported');
        if (this.formatter.type !== Format.ldjson) throw new Error('Data frame formatting is not yet supported');

        const { recordsPerChunk, chunks } = estimateRecordsPerUpload(this.slice, this.chunkSize);

        let start = 0;
        let index = 0;

        const twoMiB = this.chunkSize * 2;

        let hasMore = true;

        while (hasMore) {
            let end = recordsPerChunk + start;
            if (end > this.slice.size) end = this.slice.size;

            const records = this.slice.slice(start, end);
            const ary = records.toJSON(this.serializeOptions || {
                useNullForUndefined: false,
                skipNilValues: true,
                skipEmptyObjects: true,
                skipNilObjectValues: true,
                skipDuplicateObjects: false,
            });

            const body = ary.map((el) => JSON.stringify(el)).join('\n');

            index = index + 1;

            // should be under 1MiB hopefully but allow up to 2MiB
            if (body.length < twoMiB) {
                start = start + recordsPerChunk;

                if (chunks === 1 || start > this.slice.size) {
                    hasMore = false;
                } else {
                    const nextChunk = this.slice.slice(start);
                    hasMore = Boolean(nextChunk.size);
                }

                yield {
                    index,
                    has_more: hasMore,
                    data: body,
                };
            } else {
                // in case estimates went off limit,
                // TODO maybe split ary in half, pop off a few at a time,
                // or estimate overflow, instead of looping each record

                let str = '';
                let recordsProcessed = 0;

                for (const record of ary) {
                    recordsProcessed = recordsProcessed + 1;
                    str = str + `${JSON.stringify(record)}\n`;

                    if (str.length >= this.chunkSize) {
                        start = start + recordsProcessed;

                        if (start > this.slice.size) {
                            hasMore = false;
                        } else {
                            const nextChunk = this.slice.slice(start);
                            hasMore = Boolean(nextChunk.size);
                        }

                        yield {
                            index,
                            has_more: hasMore,
                            data: str,
                        };
                        break;
                    }
                }
                start = start + recordsProcessed;

                if (start > this.slice.size) {
                    hasMore = false;
                } else {
                    const nextChunk = this.slice.slice(start);
                    hasMore = Boolean(nextChunk.size);
                }

                yield {
                    index,
                    has_more: hasMore,
                    data: str,
                };
            }
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
