import { DataEntity } from '@terascope/utils';
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
    constructor(
        readonly formatter: Formatter,
        readonly compression: Compressor,
        readonly slice: (Record<string, unknown>|DataEntity)[]
    ) {}

    async* [Symbol.asyncIterator](): AsyncIterableIterator<Chunk> {
        if (!this.slice.length) return;
        const has_more = false;

        const formattedData = this.formatter.format(this.slice);
        const data = await this.compression.compress(formattedData);

        yield {
            index: 1,
            data,
            has_more
        };
    }
}
