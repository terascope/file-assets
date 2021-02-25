import { Logger } from '@terascope/utils';
import type S3 from 'aws-sdk/clients/s3';
import {
    FileSlice,
    ReaderConfig,
    SliceConfig,
    FileSliceConfig,
    BaseSenderConfig
} from '../interfaces';
import { segmentFile, canReadFile } from '../base';
import { S3Slicer } from './s3-slicer';
import { isObject } from '../helpers';
import { S3Fetcher } from './s3-fetcher';
import { S3Sender } from './s3-sender';

function validateSenderConfig(input: Record<string, any>) {
    if (!isObject(input)) throw new Error('Invalid config parameter, ut must be an object');
    (input as Record<string, unknown>);
    if (input.file_per_slice == null || input.file_per_slice === false) {
        throw new Error('Invalid parameter "file_per_slice", it must be set to true, cannot be append data to S3 objects');
    }
}

export class S3TerasliceAPI extends S3Fetcher {
    readonly segmentFileConfig: SliceConfig
    readonly slicerConfig: FileSliceConfig;

    constructor(client: S3, config: ReaderConfig, logger: Logger) {
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
        return new S3Slicer(this.client, this.slicerConfig, this.logger);
    }

    async makeSender(senderConfig: BaseSenderConfig): Promise<S3Sender> {
        const config = Object.assign({}, this.slicerConfig, senderConfig);
        validateSenderConfig(config);
        return new S3Sender(this.client, config, this.logger);
    }
}
