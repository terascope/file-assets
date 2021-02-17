import { AnyObject, Logger } from '@terascope/job-components';
import { SlicedFileResults, ChunkedConfig } from '../interfaces';
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

    /**
     * low level api that fetches the unprocessed contents of the file, please use the "read" method
     * for correct file and data parsing
     * @example
     * const slice = { offset: 0, length: 1000, path: 'some/file.txt', total: 1000 };
     * const results = await s3Reader.fetch(slice);
     * results === 'the unprocessed contents of the file here'
    */
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

    /**
     * Determines if a file name or file path can be processed, it will return false
     * if the name of path includes a "."
     *
     * @example
     * s3Reader.canReadFile('file.txt')  => true
     * s3Reader.canReadFile('some/path/file.txt')  => true
     * s3Reader.canReadFile('some/.private_path/file.txt')  => false
    */
    canReadFile(filePath: string): boolean {
        return canReadFile(filePath);
    }

    /**
 *  Used to slice up a file based on the configuration provided
 *
 * The returned results can be used directly with any "read" method of a reader API
 *
 * @example
 *  const slice = { path: 'some/path', size: 1000 };
    const config = {
        file_per_slice: false,
        line_delimiter: '\n',
        size: 300,
        format: Format.ldjson
    };

    const results = s3Reader.segmentFile(slice, config);

    results === [
        {
            offset: 0, length: 300, path: 'some/path', total: 1000
        },
            offset: 299, length: 301, path: 'some/path', total: 1000
        },
            offset: 599, length: 301, path: 'some/path', total: 1000
        },
        {
            offset: 899, length: 101, path: 'some/path', total: 1000
        }
    ]
 */
    segmentFile(file: {
        path: string;
        size: number;
    }): SlicedFileResults[] {
        return segmentFile(file, this.config);
    }

    /**
     * Generates a slicer based off the configs
     *
     * @example
     * const config = {
     *      size: 1000,
     *      file_per_slice: false,
     *      line_delimiter: '\n',
     *      size: 300,
     *      format: "ldjson"
     *      path: 'some/dir'
     * }
     * const s3Reader = new S3Reader(config);
     * const slicer = await s3Reader.newSlicer();
     *
     * const results = await slicer.slice();
     * results === [
     *      { offset: 0, length: 1000, path: 'some/dir/file.txt', total: 1000 }
     * ]
    */
    async makeSlicer(): Promise<S3Slicer> {
        return new S3Slicer(this.client, this.config, this.logger);
    }
}
