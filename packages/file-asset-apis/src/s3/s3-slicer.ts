import type S3 from 'aws-sdk/clients/s3';
import { flatten, Logger } from '@terascope/utils';
import { segmentFile, parsePath, canReadFile } from '../base';
import { SliceConfig, FileSlice, FileSliceConfig } from '../interfaces';
import { listS3Objects } from './s3-helpers';
import { isObject } from '../helpers';

function validateConfig(input: unknown) {
    if (!isObject(input)) throw new Error('Invalid config parameter, ut must be an object');
    (input as Record<string, unknown>);
    if (input.file_per_slice == null || input.file_per_slice === false) {
        throw new Error('Invalid parameter "file_per_slice", it must be set to true, cannot be append data to S3 objects');
    }
}

export class S3Slicer {
    readonly sliceConfig: SliceConfig;
    logger: Logger;
    client: S3;
    readonly bucket: string;
    readonly prefix: string;
    _lastKey: string | undefined;
    protected _doneSlicing = false;

    constructor(client: S3, config: FileSliceConfig, logger: Logger) {
        validateConfig(config);
        const { path, ...sliceConfig } = config;
        const { bucket, prefix } = parsePath(path);
        this.sliceConfig = sliceConfig;
        this.logger = logger;
        this.client = client;
        this.bucket = bucket;
        this.prefix = prefix;
    }

    private async getObjects(): Promise<FileSlice[]> {
        const data = await listS3Objects(this.client, {
            Bucket: this.bucket,
            Prefix: this.prefix,
            Marker: this._lastKey,
        });

        if (!data.Contents?.length) {
            // Returning an empty array will signal to the slicer that it is done
            // TODO: log a message to let the user know there weren't any slices
            return [];
        }

        this._lastKey = data.Contents[data.Contents.length - 1].Key;

        // Let slicer know whether or not there are more objects to process
        if (data.IsTruncated) {
            this._doneSlicing = false;
        } else {
            this._doneSlicing = true;
        }

        const actions = [];

        // Slice whatever objects are returned from the query
        for (const content of data.Contents) {
            if (content.Key == null) {
                this.logger.warn('Missing content.Key from S3 List Object Request');
            } else if (content.Size == null) {
                this.logger.warn('Missing content.Size from S3 List Object Request');
            } else if (canReadFile(content.Key)) {
                actions.push(segmentFile({
                    path: content.Key,
                    size: content.Size
                }, this.sliceConfig));
            } else {
                this.logger.warn(`Invalid path ${content.Key}, cannot start with a dot in directory of file name, skipping path`);
            }
        }

        const results = await Promise.all(actions);

        return flatten(results);
    }

    /**
    * This method will return an array of file slices, or null if the slicer is done
    */
    async slice(): Promise<FileSlice[]|null> {
        // First check to see if there are more objects in S3
        if (this._doneSlicing) return null;

        // Get an array of slices
        const slices = await this.getObjects();

        // Finish slicer if there are no slices.
        if (slices.length === 0) return null;

        return slices;
    }
}
