import { DataEntity } from '@terascope/utils';
import { CompressionFormatter } from './compression';
import { FileFormatter } from './file-formatter';

export interface Chunk {
    /**
     * The ordered index, starts at 0, then incremented per chunk
    */
    readonly index: number;

    /**
     * The chunk of data to published
    */
    readonly chunk: Buffer;

    /**
     * Indicates whether there are more chunks to be processed
    */
    readonly has_more: boolean;
}

export class ChunkGenerator {
    constructor(
        readonly formatter: FileFormatter,
        readonly compression: CompressionFormatter,
        readonly slice: (Record<string, any>|DataEntity)[]
    ) {}

    * [Symbol.iterator](): IterableIterator<Chunk> {
        yield* [];
    }
}
