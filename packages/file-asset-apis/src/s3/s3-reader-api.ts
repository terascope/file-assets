import { Logger } from '@terascope/core-utils';
import type { S3Client } from './client-helpers/index.js';
import {
    FileSlice,
    ReaderAPIConfig,
    SliceConfig,
    FileSliceConfig,
} from '../interfaces.js';
import { segmentFile, canReadFile } from '../base/index.js';
import { S3Slicer } from './s3-slicer.js';
import { S3Fetcher } from './s3-fetcher.js';

export class S3TerasliceAPI extends S3Fetcher {
    readonly segmentFileConfig: SliceConfig;
    readonly slicerConfig: FileSliceConfig;

    constructor(client: S3Client, config: ReaderAPIConfig, logger: Logger) {
        super(client, config, logger);
        const { path, size } = config;
        const { lineDelimiter, format, filePerSlice } = this;

        this.segmentFileConfig = {
            line_delimiter: lineDelimiter,
            format,
            size,
            file_per_slice: filePerSlice
        };

        this.slicerConfig = {
            path,
            ...this.segmentFileConfig
        };
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
        return segmentFile(file, this.segmentFileConfig);
    }

    /**
     * Generates a function that will resolve one or more slices each time it is called.
     * These slices will can be used to "fetch" chunks of data. Returns `null` when complete
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
     *   const results = await slicer();
     *   results === [
     *      { offset: 0, length: 1000, path: 'some/dir/file.txt', total: 1000 }
     *   ]
    */
    async makeSlicer(): Promise<() => Promise<FileSlice[] | null>> {
        const slicer = new S3Slicer(this.client, this.slicerConfig, this.logger);
        return async function _slice() {
            return slicer.slice();
        };
    }
}
