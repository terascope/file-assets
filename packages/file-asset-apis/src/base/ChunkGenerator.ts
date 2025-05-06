import { isTest } from '@terascope/utils';
import { type DataFrame } from '@terascope/data-mate';
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

    /**
     * Cleanup function  - i.e. to free up memory
     */
    readonly cleanup?: (idx: number) => void;
}

export const MiB = 1024 * 1024;

/** 100 MiB - Used for determine how big each chunk of a single file should be */
export const MAX_CHUNK_SIZE_BYTES = (isTest ? 10 : 100) * MiB;

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
    readonly experimentalChunkMethod?: 'buffer' | 'batchBuffer' | 'batchStr';
    dataFrame?: DataFrame;

    constructor(
        readonly formatter: Formatter,
        readonly compressor: Compressor,
        readonly slice: SendRecords,
        experimentalChunkMethod?: 'buffer' | 'batchBuffer' | 'batchStr',
        limits?: { maxBytes?: number; minBytes?: number },
        dataFrame?: DataFrame
    ) {
        const max = limits?.maxBytes || MAX_CHUNK_SIZE_BYTES;
        const min = limits?.minBytes || MIN_CHUNK_SIZE_BYTES;
        this.chunkSize = getBytes(max, min);
        this.experimentalChunkMethod = experimentalChunkMethod;
        this.dataFrame = dataFrame;
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
        if (this.dataFrame) {
            if (this.experimentalChunkMethod === 'buffer' || this.experimentalChunkMethod === 'batchBuffer') return this._chunkDataFrameBuffer();
            if (this.experimentalChunkMethod === 'batchStr') this._chunkDataFrameStrBatched();
            return this._chunkDataFrameStrNonBatch();
        }
        if (Array.isArray(this.slice) && this.slice.length === 0) {
            return this._emptyIterator();
        }
        if (this.experimentalChunkMethod === 'buffer') return this._chunkBuffer();

        if (this.isRowOptimized()) {
            if (this.experimentalChunkMethod === 'batchBuffer') return this._chunkBufferBatched();
            return this._chunkByRow();
        }
        if (this.experimentalChunkMethod === 'batchBuffer') throw new Error('Experimental method only supported in ldjson format, no compression');
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
                    data: chunkStr.slice(0, this.chunkSize)
                };

                chunkStr = chunkStr.slice(this.chunkSize, chunkStr.length);
                index++;
            } else if (estimatedOverflowBytes >= 0) {
                chunk = {
                    index,
                    has_more,
                    data: chunkStr
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

    private async* _chunkBuffer(): AsyncIterableIterator<Chunk> {
        let index = 0;
        const buffers: Record<number, Buffer> = {
            0: Buffer.alloc(this.chunkSize)
        };
        const sizes: Record<number, number> = {
            0: 0
        };
        const cleanup = (idx: number) => {
            delete buffers[idx];
            delete sizes[idx];
        };
        const startNextBuffer = () => {
            index++;
            buffers[index] = Buffer.alloc(this.chunkSize);
            sizes[index] = 0;
        };

        const estimates = { count: 0, total: 0, average: 0 };

        // eslint-disable-next-line prefer-const
        for (let [str, has_more] of this.formatter.formatIterator(this.slice)) {
            let chunk;

            if (!estimates.average && str.length) {
                estimates.count = estimates.count + 1;
                estimates.total = estimates.total + str.length;

                const almostFull = estimates.total + str.length >= this.chunkSize;
                if (almostFull || estimates.count >= 5) {
                    estimates.average = estimates.total / estimates.count;
                }
            }

            // if doesn't fit - make chunk, start next buffer, add to next buffer
            // if fits, but next string won't, add to buffer, make chunk, start next buffer
            // else buffer not close to full so just write to buffer
            if (sizes[index] + str.length > this.chunkSize) {
                chunk = {
                    index,
                    has_more,
                    data: buffers[index].subarray(0, sizes[index]),
                    cleanup
                };
                startNextBuffer();
                const size = buffers[index].write(str, sizes[index]);
                sizes[index] = sizes[index] + size;
            } else if (
                sizes[index] + str.length + estimates.average >= this.chunkSize
                && sizes[index] + str.length <= this.chunkSize
            ) {
                const size = buffers[index].write(str, sizes[index]);
                sizes[index] = sizes[index] + size;
                chunk = {
                    index,
                    has_more,
                    data: buffers[index].subarray(0, sizes[index]),
                    cleanup
                };
                startNextBuffer();
            } else {
                const size = buffers[index].write(str, sizes[index]);
                sizes[index] = sizes[index] + size;
            }

            str = ''; // clear memory

            if (chunk) {
                yield chunk;
            }
        }
        if (sizes[index]) {
            yield {
                index,
                has_more: false,
                data: buffers[index].subarray(0, sizes[index]),
                cleanup
            };
        }
    }

    private async* _chunkBufferBatched(): AsyncIterableIterator<Chunk> {
        let index = 0;
        const buffers: Record<number, Buffer> = { 0: Buffer.alloc(this.chunkSize) };
        const sizes: Record<number, number> = { 0: 0 };

        const cleanup = (idx: number) => {
            delete buffers[idx];
            delete sizes[idx];
        };
        const startNextBuffer = () => {
            index++;
            buffers[index] = Buffer.alloc(this.chunkSize);
            sizes[index] = 0;
        };

        let items: (Record<string, any> | string)[] = [];

        let batchSize = Number(process.argv.find((el) => el.startsWith('batch'))?.split('=')[1] || 100);
        let avgBatchSize = 0;

        const estimates = { count: 0, total: 0, average: 0, ready: false };

        for (const [record, has_more] of hasMoreIterator(this.slice)) {
            let wrote = false;
            if (!estimates.ready && record) {
                // FIXME maybe consider adding a let wrote; outside if, write to buffer,
                // and at bottom if (!wrote) items.push at end
                const formatted = `${JSON.stringify(record)}\n`;
                const size = buffers[index].write(formatted, sizes[index]);
                sizes[index] = sizes[index] + size;
                wrote = true;
                estimates.count = estimates.count + 1;
                estimates.total = estimates.total + formatted.length;
                estimates.average = estimates.total / estimates.count;

                const almostFull = estimates.total + formatted.length >= this.chunkSize;
                if (almostFull || estimates.count >= 5) {
                    estimates.ready = true;
                    estimates.average = estimates.total / estimates.count;
                    ({
                        batchSize, avgBatchSize, // estimatedBatchesPerUpload,
                    } = this._ensureBatchSizeOk(estimates.average, items));
                }
            }

            // add item batch to buffer
            if (items.length && items.length % batchSize === 0) {
                // NOTE: cannot join on '\n' - converts array to string
                // which would be '[object Object]\n[object Object]'
                const stringified: string | null = JSON.stringify!(items)
                    .replaceAll(/,"\\n",*/g, '\n')
                    .replace(/^\[/g, '')
                    .replace(/]$/g, '');

                const size = buffers[index].write(stringified, sizes[index], 'utf-8');
                sizes[index] = sizes[index] + size;
                items = [];
            }

            // FIXME - add something to not yielding < 5mib
            // will overflow next item batch so yield
            if (sizes[index] && sizes[index] + avgBatchSize >= this.chunkSize) {
                yield {
                    index,
                    has_more,
                    data: buffers[index].subarray(0, sizes[index]),
                    cleanup
                };
                startNextBuffer();
            }

            if (!wrote) {
            // stringify in batches - can't join on '\n' so push '\n'
                items.push(record);
                items.push('\n');
            }
        }

        if (items.length) {
            const stringified: string | null = JSON.stringify!(items)
                .replaceAll(/,"\\n",*/g, '\n')
                .replace(/^\[/g, '')
                .replace(/]$/g, '');

            const size = buffers[index].write(stringified, sizes[index], 'utf-8');
            sizes[index] = sizes[index] + size;
            items = [];
        }
        if (sizes[index]) {
            yield {
                index,
                has_more: false,
                data: buffers[index].subarray(0, sizes[index]),
                cleanup
            };
        }
    }

    private _ensureBatchSizeOk(avgRecordBytes: number, items: any[]) {
        let estimatedBatchesPerUpload = 0;

        let batchSize = 100;

        // ensure batch size ok
        if (!avgRecordBytes && items.length >= 5) {
            const isValidBatchSize = () => {
                // FIXME - wound up getting Infinity in some scenarios
                const batchesPerChunk = this.chunkSize / (batchSize * avgRecordBytes);
                if (batchesPerChunk > this.chunkSize) return false;
                return batchesPerChunk;
            };

            while (!estimatedBatchesPerUpload) {
                if (isValidBatchSize()) {
                    estimatedBatchesPerUpload = this.chunkSize / batchSize;
                } else if (batchSize === 1) {
                    estimatedBatchesPerUpload = 1;
                } else {
                    // FIXME - wound up getting0 batchSize in some scenarios
                    batchSize = batchSize / 10;
                }
            }
        }

        return {
            estimatedBatchesPerUpload,
            avgRecordBytes,
            batchSize,
            avgBatchSize: avgRecordBytes * batchSize
        };
    }

    private async* _emptyIterator(): AsyncIterableIterator<Chunk> {}

    private async* _chunkDataFrameBuffer(): AsyncIterableIterator<Chunk> {
        let index = 0;
        const buffers: Record<number, Buffer> = { 0: Buffer.alloc(this.chunkSize) };
        const sizes: Record<number, number> = { 0: 0 };

        const cleanup = (idx: number) => {
            delete buffers[idx];
            delete sizes[idx];
        };
        const startNextBuffer = () => {
            index++;
            buffers[index] = Buffer.alloc(this.chunkSize);
            sizes[index] = 0;
        };

        let items: (Record<string, any> | string)[] = [];

        let batchSize = Number(process.argv.find((el) => el.startsWith('batch'))?.split('=')[1] || 100);
        let avgBatchSize = 0;

        const estimates = { count: 0, total: 0, average: 0, ready: false };

        let start = 0;

        let done = false;
        while (!done) {
            let end = start + batchSize;
            if (end > this.dataFrame!.size) end = this.dataFrame!.size;
            const records = this.dataFrame!.slice(start, end);
            start = start + batchSize;
            if (start > this.dataFrame!.size) done = true;
            let batchDone = false;
            let batched = 0;

            for (const record of records) {
                batched += 1;
                if (batched >= records.size) {
                    batchDone = true;
                }
                let wrote = false;
                if (!estimates.ready && record) {
                    const formatted = `${JSON.stringify(record)}\n`;
                    const size = buffers[index].write(formatted, sizes[index]);
                    sizes[index] = sizes[index] + size;
                    wrote = true;
                    estimates.count = estimates.count + 1;
                    estimates.total = estimates.total + formatted.length;
                    estimates.average = estimates.total / estimates.count;

                    const almostFull = (estimates.total + formatted.length) >= this.chunkSize;
                    // let estimatedBatchesPerUpload;
                    if (almostFull || estimates.count >= 5) {
                        estimates.ready = true;
                        ({
                            batchSize, avgBatchSize, // estimatedBatchesPerUpload,
                        } = this._ensureBatchSizeOk(estimates.average, items));
                    }
                }

                // add item batch to str or buffer
                if (items.length && items.length % batchSize === 0) {
                    const formatted = JSON.stringify!(items)
                        .replaceAll(/,"\\n",*/g, '\n')
                        .replace(/^\[/g, '')
                        .replace(/]$/g, '');
                    const size = buffers[index].write(formatted, sizes[index]);
                    sizes[index] = sizes[index] + size;
                    items = [];
                }

                // will overflow next item batch so yield
                if (sizes[index] + avgBatchSize >= this.chunkSize) {
                    if (!avgBatchSize) throw new Error('foo');
                    yield {
                        index,
                        has_more: !done && !batchDone,
                        data: buffers[index].subarray(0, sizes[index]),
                        cleanup
                    };
                    startNextBuffer();
                }

                if (!wrote) {
                    // stringify in batches - can't join on '\n' so push '\n'
                    items.push(record);
                    items.push('\n');
                }
            }
        }
        if (items.length) {
            const formatted = JSON.stringify!(items)
                .replaceAll(/,"\\n",*/g, '\n')
                .replace(/^\[/g, '')
                .replace(/]$/g, '');
            const size = buffers[index].write(formatted, sizes[index]);
            sizes[index] = sizes[index] + size;
            items = [];
        }
        if (sizes[index]) {
            yield {
                index,
                has_more: false,
                data: buffers[index].subarray(0, sizes[index]),
                cleanup
            };
        }
    }

    private async* _chunkDataFrameStrBatched(): AsyncIterableIterator<Chunk> {
        let items: (Record<string, any> | string)[] = [];
        let str = '';
        let index = 0;

        let batchSize = Number(process.argv.find((el) => el.startsWith('batch'))?.split('=')[1] || 100);
        let avgBatchSize = 0;

        const estimates = { count: 0, total: 0, average: 0, ready: false };

        let start = 0;

        let done = false;
        while (!done) {
            let end = start + batchSize;
            if (end > this.dataFrame!.size) end = this.dataFrame!.size;
            const records = this.dataFrame!.slice(start, end);
            start = start + batchSize;
            if (start > this.dataFrame!.size) done = true;
            let batchDone = false;
            let batched = 0;

            for (const record of records) {
                let chunk;
                batched += 1;
                if (batched >= records.size) {
                    batchDone = true;
                }
                let wrote = false;
                if (!estimates.ready && record) {
                    const formatted = `${JSON.stringify(record)}\n`;
                    str += formatted;
                    wrote = true;
                    estimates.count = estimates.count + 1;
                    estimates.total = estimates.total + formatted.length;
                    estimates.average = estimates.total / estimates.count;

                    const almostFull = (estimates.total + formatted.length) >= this.chunkSize;
                    // let estimatedBatchesPerUpload;
                    if (almostFull || estimates.count >= 5) {
                        estimates.ready = true;
                        ({
                            batchSize, avgBatchSize, // estimatedBatchesPerUpload,
                        } = this._ensureBatchSizeOk(estimates.average, items));
                    }
                }

                // add item batch to str or buffer
                if (items.length && items.length % batchSize === 0) {
                    str += JSON.stringify!(items)
                        .replaceAll(/,"\\n",*/g, '\n')
                        .replace(/^\[/g, '')
                        .replace(/]$/g, '');

                    items = [];
                }

                // will overflow next item batch so yield
                if (str.length + avgBatchSize >= this.chunkSize) {
                    if (!avgBatchSize) throw new Error('foo');
                    chunk = {
                        index,
                        has_more: !done && !batchDone,
                        data: str,
                    };
                    index += 1;
                    str = '';
                }

                if (!wrote) {
                    // stringify in batches - can't join on '\n' so push '\n'
                    items.push(record);
                    items.push('\n');
                }
                if (chunk) yield chunk;
            }
        }
        if (items.length) {
            str += JSON.stringify!(items)
                .replaceAll(/,"\\n",*/g, '\n')
                .replace(/^\[/g, '')
                .replace(/]$/g, '');
        }
        if (str.length) {
            yield {
                index,
                has_more: false,
                data: str,

            };
            str = '';
        }
    }

    private async* _chunkDataFrameStrNonBatch(): AsyncIterableIterator<Chunk> {
        const batchSize = Number(process.argv.find((el) => el.startsWith('batch'))?.split('=')[1] || 100);

        let start = 0;

        let done = false;
        let index = 0;
        let chunkStr = '';

        let chunk: Chunk | undefined;

        while (!done) {
            let end = start + batchSize;
            if (end > this.dataFrame!.size) end = this.dataFrame!.size;
            const records = this.dataFrame!.slice(start, end);
            start = start + batchSize;
            if (start > this.dataFrame!.size) done = true;
            let batchDone = false;
            let batched = 0;

            for (const record of records) {
                batched++;
                if (batched >= records.size) batchDone = true;
                chunk = undefined;

                chunkStr += `${JSON.stringify(record)}\n`;

                const estimatedOverflowBytes = chunkStr.length - this.chunkSize;
                if (estimatedOverflowBytes >= this.chunkSize) {
                    chunk = {
                        index,
                        has_more: !done && !batchDone,
                        data: chunkStr.slice(0, this.chunkSize)
                    };

                    chunkStr = chunkStr.slice(this.chunkSize, chunkStr.length);
                    index++;
                } else if (estimatedOverflowBytes >= 0) {
                    chunk = {
                        index,
                        has_more: !done && !batchDone,
                        data: chunkStr
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
    }
}

/**
 * Get bytes with a hard minimum
*/
function getBytes(max: number, min: number): number {
    return Math.max(max, min);
}
