import { Logger } from '@terascope/job-components';
import type S3 from 'aws-sdk/clients/s3';
import { FileSlice, ChunkedConfig } from '../interfaces';
import {
    ChunkedFileReader, segmentFile, canReadFile, parsePath
} from '../base';
import { S3Slicer } from './s3-slicer';
import { getS3Object } from './helpers';

export class S3Reader extends ChunkedFileReader {
    client: S3;
    bucket: string;

    constructor(client: S3, config: ChunkedConfig, logger: Logger) {
        super(config, logger);
        const { bucket } = parsePath(this.config.path);
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
        return this.decompress(results.Body);
    }

    /**
     * Determines if a file name or file path can be processed, it will return false
     * if the name of path contains a segment that starts with "."
     *
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
 *   const slice = { path: 'some/path', size: 1000 };
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
    }): FileSlice[] {
        return segmentFile(file, this.config);
    }

    /**
     * Generates a slicer based off the configs
     *
     * @example
     *   const config = {
     *      size: 1000,
     *      file_per_slice: false,
     *      line_delimiter: '\n',
     *      size: 300,
     *      format: "ldjson"
     *      path: 'some/dir'
     *   }
     *   const s3Reader = new S3Reader(config);
     *   const slicer = await s3Reader.newSlicer();
     *
     *   const results = await slicer.slice();
     *   results === [
     *      { offset: 0, length: 1000, path: 'some/dir/file.txt', total: 1000 }
     *   ]
    */
    async makeSlicer(): Promise<S3Slicer> {
        return new S3Slicer(this.client, this.config, this.logger);
    }
}
