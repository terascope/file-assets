import { gzip, ungzip } from 'node-gzip';
// @ts-expect-error
import { encode, decode } from 'lz4';
import { Compression } from '../interfaces';

const allowedKey = Object.values(Compression);

async function lz4Compress(data: unknown) {
    return encode(data);
}

async function gzipCompress(data: unknown) {
    return gzip(data as any);
}

async function noneCompress(data: unknown) {
    return data;
}

async function lz4Decompress(data: unknown) {
    return decode(data).toString();
}

async function gzipDecompress(data: unknown) {
    const uncompressed = await ungzip(data as any);
    return uncompressed.toString();
}

async function noneDecompress(data: unknown) {
    return (data as any).toString();
}

export default class CompressionFormatter {
    readonly compression: Compression;
    compress!: (data:unknown) => Promise<Buffer| any>;
    decompress!: (data:unknown)=> Promise<string>;

    constructor(format: Compression) {
        if (!allowedKey.includes(format)) throw new Error(`Unsupported compression: ${format}`);
        this.compression = format;

        if (this.compression === Compression.lz4) {
            this.compress = lz4Compress;
            this.decompress = lz4Decompress;
        }

        if (this.compression === Compression.gzip) {
            this.compress = gzipCompress;
            this.decompress = gzipDecompress;
        }

        if (this.compression === Compression.none) {
            this.compress = noneCompress;
            this.decompress = noneDecompress;
        }
    }
}
