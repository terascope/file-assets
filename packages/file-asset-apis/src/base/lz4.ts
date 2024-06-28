// @ts-expect-error
import lz4init from 'lz4-asm/dist/lz4asm.js';

export interface LZ4 {
    compress(data: Buffer): Promise<Buffer>;
    decompress(data: Buffer|string): Buffer;
}

const lz4Module = {};
let lz4js: LZ4|undefined;

export async function getLZ4(): Promise<LZ4> {
    if (lz4js) return lz4js;

    const lz4Ready = lz4init(lz4Module);
    lz4js = (await lz4Ready).lz4js;
    return lz4js!;
}
