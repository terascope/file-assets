import {
    Fetcher, getClient, WorkerContext, ExecutionConfig
} from '@terascope/job-components';
import path from 'path';
import { S3ReaderConfig } from './interfaces';
import { getChunk } from '../__lib/chunked-file-reader';
import { decompress } from '../__lib/compression';
import { parsePath } from '../__lib/fileName';
import { SlicedFileResults } from '../__lib/slice';

export default class S3Fetcher extends Fetcher<S3ReaderConfig> {
    client: any;
    bucket: string;
    prefix: string;

    constructor(context: WorkerContext, opConfig: S3ReaderConfig, exeConfig: ExecutionConfig) {
        super(context, opConfig, exeConfig);
        this.client = getClient(context, opConfig, 's3');
        const { bucket, prefix } = parsePath(opConfig.path);
        this.bucket = bucket;
        this.prefix = prefix;
    }

    async fetch(slice: SlicedFileResults) {
        const { compression } = this.opConfig;
        // Coerce the field delimiter if the format is `tsv`
        const reader = async (offset: number, length: number) => {
            const opts = {
                Bucket: this.bucket,
                // TODO: figure out way to not shaeow slice.path here
                Key: path.join(this.prefix, path.basename(slice.path)),
                // We need to subtract 1 from the range in order to avoid collecting an extra byte.
                // i.e. Requesting only the first byte of a file has a `length` of `1`, but the
                //   request would be for `bytes=0-0`
                Range: `bytes=${offset}-${offset + length - 1}`
            };
            /* The object returned looks something like this:
             * {
             *   AcceptRanges: 'bytes',
             *   LastModified: 2019-07-19T22:27:11.000Z,
             *   ContentLength: 51,
             *   ETag: '"xxxx"',
             *   ContentRange: 'bytes 44-94/98',
             *   ContentType: 'text/csv',
             *   Metadata: {
             *     's3cmd-attrs': '...'
             *   },
             *   Body: <Buffer FF FF ... >
             * }
             */
            const results = await this.client.getObject_Async(opts);
            return decompress(results.Body, compression);
        };
        // Passing the slice in as the `metadata`. This will include the path, offset, and length
        return getChunk(reader, slice, this.opConfig, this.logger, slice);
    }
}
