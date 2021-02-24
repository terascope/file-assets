import { Logger } from '@terascope/utils';
import type S3 from 'aws-sdk/clients/s3';
import { FileSlice, S3FetcherConfig } from '../interfaces';
import { ChunkedFileReader, parsePath } from '../base';
import { getS3Object } from './s3-helpers';
import { isObject } from '../helpers';

function validateConfig(input: unknown) {
    if (!isObject(input)) throw new Error('Invalid config parameter, ut must be an object');
    (input as Record<string, unknown>);
    if (input.file_per_slice == null || input.file_per_slice === false) {
        throw new Error('Invalid parameter "file_per_slice", it must be set to true, cannot be append data to S3 objects');
    }
}

export class S3Fetcher extends ChunkedFileReader {
    protected client: S3;
    protected bucket: string;

    constructor(client: S3, config: S3FetcherConfig, logger: Logger) {
        validateConfig(config);
        super(config, logger);
        const { path } = config;
        const { bucket } = parsePath(path);
        this.client = client;
        this.bucket = bucket;
    }

    /**
     * low level api that fetches the unprocessed contents of the file, please use the "read" method
     * for correct file and data parsing
     * @example
     *   const slice = { offset: 0, length: 1000, path: 'some/file.txt', total: 1000 };
     *   const results = await s3Reader.fetch(slice);
     *   results === 'the unprocessed contents of the file here'
    */
    protected async fetch(slice: FileSlice): Promise<string> {
        const { offset, length } = slice;
        const results = await getS3Object(this.client, {
            Bucket: this.bucket,
            Key: slice.path,
            // We need to subtract 1 from the range in order to avoid collecting an extra byte.
            // i.e. Requesting only the first byte of a file has a `length` of `1`, but the
            //   request would be for `bytes=0-0`
            Range: `bytes=${offset}-${offset + length - 1}`
        });

        if (!results.Body) {
            throw new Error('Missing body from s3 get object request');
        }

        return this.decompress(
            Buffer.isBuffer(results.Body)
                ? results.Body
                : Buffer.from(results.Body as any)
        );
    }
}
