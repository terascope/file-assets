import { gzip, ungzip } from 'node-gzip';
// @ts-ignore
import { encode, decode } from 'lz4';
import { Compression } from './interfaces';

export async function compress(data: any, compression: Compression): Promise<Buffer| any> {
    switch (compression) {
        case 'lz4':
            return encode(data);
        case 'gzip':
            return gzip(data);
        case 'none':
            return data;
        default:
        // This shouldn't happen since the config schemas will protect against it
            throw new Error(`Unsupported compression: ${compression}`);
    }
}

export async function decompress(data: any, compression: Compression): Promise<string> {
    switch (compression) {
        case 'lz4':
            return decode(data).toString();
        case 'gzip':
            return ungzip(data).then((uncompressed: any) => uncompressed.toString());
        case 'none':
            return data.toString();
        default:
        // This shouldn't happen since the config schemas will protect against it
            throw new Error(`Unsupported compression: ${compression}`);
    }
}
