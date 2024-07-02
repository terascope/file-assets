import pkg from 'node-gzip';
import { Compression } from '../interfaces.js';
import { getLZ4 } from './lz4.js';

const { gzip, ungzip } = pkg;

async function lz4Compress(data: Buffer|string): Promise<Buffer> {
    const { compress } = await getLZ4();
    return compress(
        Buffer.isBuffer(data)
            ? data
            : Buffer.from(data)
    );
}

async function gzipCompress(data: Buffer|string): Promise<Buffer> {
    return gzip(data);
}

async function noneCompress(data: Buffer|string): Promise<Buffer> {
    return Buffer.isBuffer(data)
        ? data
        : Buffer.from(data);
}

async function lz4Decompress(data: Buffer|string): Promise<string> {
    const { decompress } = await getLZ4();
    const buf = decompress(data);
    return buf.toString();
}

async function gzipDecompress(data: Buffer|string): Promise<string> {
    const uncompressed = await ungzip(data as any);
    return uncompressed.toString();
}

async function noneDecompress(data: Buffer|string): Promise<string> {
    return Buffer.isBuffer(data) ? data.toString() : data;
}

export class Compressor {
    readonly type: Compression;
    compress: (data: Buffer|string) => Promise<Buffer>;
    decompress: (data: Buffer|string) => Promise<string>;

    constructor(type: Compression = Compression.none) {
        this.type = type;

        if (this.type === Compression.lz4) {
            this.compress = lz4Compress;
            this.decompress = lz4Decompress;
        } else if (this.type === Compression.gzip) {
            this.compress = gzipCompress;
            this.decompress = gzipDecompress;
        } else if (this.type === Compression.none) {
            this.compress = noneCompress;
            this.decompress = noneDecompress;
        } else {
            throw new Error(`Unsupported compression: ${this.type}`);
        }
    }
}
