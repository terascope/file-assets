import { toString } from '@terascope/job-components';
import { gzip, ungzip } from 'node-gzip';
// @ts-expect-error
import { encode, decode } from 'lz4';
import { Compression } from './interfaces';

export async function compress(data: unknown, compression: Compression): Promise<Buffer| any> {
    switch (compression) {
        case 'lz4':
            return encode(data);
        case 'gzip':
            return gzip(data as any);
        case 'none':
            return data;
        default:
        // This shouldn't happen since the config schemas will protect against it
            throw new Error(`Unsupported compression: ${compression}`);
    }
}

export async function decompress(data: unknown, compression: Compression): Promise<string> {
    switch (compression) {
        case 'lz4':
            return decode(data).toString();
        case 'gzip':
            return ungzip(data as any).then((uncompressed) => uncompressed.toString());
        case 'none':
            return toString(data);
        default:
        // This shouldn't happen since the config schemas will protect against it
            throw new Error(`Unsupported compression: ${compression}`);
    }
}
