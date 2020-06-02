import {
    Fetcher, getClient, WorkerContext, ExecutionConfig, DataEntity
} from '@terascope/job-components';
import { S3ReaderConfig } from './interfaces';
import { getChunk, FetcherFn } from '../__lib/chunked-file-reader';
import { decompress } from '../__lib/compression';
import { parsePath } from '../__lib/fileName';
import { SlicedFileResults } from '../__lib/interfaces';

export default class S3Fetcher extends Fetcher<S3ReaderConfig> {
    client: any;
    bucket: string;
    prefix: string;
    reader: FetcherFn;

    constructor(context: WorkerContext, opConfig: S3ReaderConfig, exeConfig: ExecutionConfig) {
        super(context, opConfig, exeConfig);
        this.client = getClient(context, opConfig, 's3');
        const { bucket, prefix } = parsePath(opConfig.path);
        this.bucket = bucket;
        this.prefix = prefix;
        this.reader = this.s3Reader.bind(this);
    }

    async s3Reader(slice: SlicedFileResults): Promise<any> {
        const { offset, length } = slice;
        const opts = {
            Bucket: this.bucket,
            Key: slice.path,
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
        return decompress(results.Body, this.opConfig.compression);
    }

    async fetch(slice: SlicedFileResults): Promise<DataEntity[]> {
        return getChunk(this.reader, this.opConfig, this.logger, slice);
    }
}
