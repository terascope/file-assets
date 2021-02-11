import { AnyObject, Logger } from '@terascope/job-components';
import {
    SlicedFileResults,
    ChunkedConfig,
    SliceConfig,
    FileSliceConfig
} from '../interfaces';
import {
    ChunkedFileReader, segmentFile, canReadFile, parsePath
} from '../lib';
import { S3Slicer } from './s3-slicer';

export class S3Reader extends ChunkedFileReader {
    client: AnyObject
    bucket: string;

    constructor(client: AnyObject, config: ChunkedConfig, logger: Logger) {
        super(config, logger);
        const { bucket } = parsePath(this.config.path);
        this.client = client;
        this.bucket = bucket;
    }

    async fetch(slice: SlicedFileResults): Promise<string> {
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
        return this.decompress(results.Body);
    }

    canReadFile(filePath: string): boolean {
        return canReadFile(filePath);
    }

    segmentFile(file: {
        path: string;
        size: number;
    }, config: SliceConfig): SlicedFileResults[] {
        return segmentFile(file, config);
    }

    async makeSlicer(config: FileSliceConfig): Promise<S3Slicer> {
        return new S3Slicer(this.client, config, this.logger);
    }
}
