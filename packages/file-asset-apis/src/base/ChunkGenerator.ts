import { isTest } from '@terascope/utils';
import { Compressor } from './Compressor.js';
import { Formatter, hasMoreIterator } from './Formatter.js';
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

export const MiB = 1024 * 1024;

/** 100 MiB - Used for determine how big each chunk of a single file should be */
export const MAX_CHUNK_SIZE_BYTES = (isTest ? 6 : 100) * MiB;

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
    readonly chunkSize: number;
    batchSize: number;
    readonly useExperimentalLDJSON?: boolean;

    constructor(
        readonly formatter: Formatter,
        readonly compressor: Compressor,
        readonly slice: SendRecords,
        limits?: { maxBytes?: number; minBytes?: number },
        useExperimentalLDJSON?: boolean,
        batchSize?: number
    ) {
        const max = limits?.maxBytes || MAX_CHUNK_SIZE_BYTES;
        const min = limits?.minBytes || MIN_CHUNK_SIZE_BYTES;
        this.chunkSize = getBytes(max, min);
        this.batchSize = batchSize || 1000;
        this.useExperimentalLDJSON = useExperimentalLDJSON;
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
        if (this.useExperimentalLDJSON) {
            if (this.formatter.type !== Format.ldjson) throw new Error('Experimental LDJSON optimization only supports ldjson');
            if (this.compressor.type !== Compression.none) throw new Error('Experimental LDJSON optimization does not supports compression');
            return this._chunkStringBatched();
        }
        if (this.isRowOptimized()) {
            return this._chunkByRow();
        }
        return this._chunkAll();
    }

    async* _chunkStringBatched() {
        let index = 0;
        let str = '';
        let items = [];

        let avgBatchSize = 0;
        const estimates = { count: 0, total: 0, average: 0, ready: false };

        for (const [record, has_more] of hasMoreIterator(this.slice)) {
            let wrote = false;

            if (!estimates.ready && record) {
                const formatted = `${JSON.stringify(record)}\n`;
                str += `${JSON.stringify(record)}\n`;
                wrote = true;

                estimates.count += 1;
                estimates.total += formatted.length;
                estimates.average = estimates.total / estimates.count;

                const almostFull = estimates.total + formatted.length >= this.chunkSize;
                if (almostFull || estimates.count >= 5) {
                    estimates.ready = true;
                    (avgBatchSize = this._fixBatchSize(estimates.average));
                }
            }

            if (items.length && items.length % this.batchSize === 0) {
                // NOTE: cannot join on '\n' - converts array to string
                // which would be '[object Object]\n[object Object]'
                str += JSON.stringify(items)
                    .replaceAll(/,"\\n",*/g, '\n')
                    .replace(/^\[/g, '')
                    .replace(/]$/g, '');
                items = [];
            }

            // can overflow next item so yield
            if (str.length + avgBatchSize >= this.chunkSize) {
                yield {
                    index,
                    has_more,
                    data: str,
                };
                index++;
                str = '';
            }

            if (!wrote) {
                // stringify in batches - can't join on '\n' so push '\n'
                items.push(record);
                items.push('\n');
            }
        }

        // finish off anything left
        if (items.length) {
            str += JSON.stringify(items)
                .replaceAll(/,"\\n",*/g, '\n')
                .replace(/^\[/g, '')
                .replace(/]$/g, '');
            items = [];
        }
        if (str) {
            yield {
                index,
                has_more: false,
                data: str,
            };
            str = '';
        }
    }

    /** Adjusts the batch size if needed to ensure it stays under the chunk size */
    _fixBatchSize(_avgRecordBytes: number) {
        const avgRecordBytes = _avgRecordBytes || 1; // ensure > 0
        let done = false;
        this.batchSize = this.batchSize || 2000; // ensure > 0
        let avgBatchSize = 0;

        const isValidBatchSize = () => {
            avgBatchSize = this.batchSize * avgRecordBytes;
            return avgBatchSize < this.chunkSize;
        };

        while (!done) {
            if (isValidBatchSize()) {
                done = true;
            } else if (this.batchSize <= 1) {
                this.batchSize = 1;
                done = true;
            } else {
                this.batchSize = Math.floor(this.batchSize / 10);
            }
        }

        return avgBatchSize;
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
