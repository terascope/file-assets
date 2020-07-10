import { AnyObject, Logger } from '@terascope/job-components';
import { SlicedFileResults } from '../__lib/interfaces';
import ChunkedReader from '../__lib/chunked-file-reader';

export default class S3Reader extends ChunkedReader {
    client: AnyObject

    constructor(client: AnyObject, config: AnyObject, logger: Logger) {
        super(config, logger);
        this.client = client;
    }

    async fetch(slice: SlicedFileResults): Promise<string> {
        const { offset, length } = slice;
        const opts = {
            Bucket: this.config.bucket,
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
}
