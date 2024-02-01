import { flatten, Logger } from '@terascope/utils';
import type { S3Client, S3ClientResponse } from './client-types';
import { segmentFile, parsePath, canReadFile } from '../base';
import { SliceConfig, FileSlice, FileSliceConfig } from '../interfaces';
import { listS3Objects, s3RequestWithRetry } from './s3-helpers';

export class S3Slicer {
    readonly sliceConfig: SliceConfig;
    logger: Logger;
    client: S3Client;
    readonly bucket: string;
    readonly prefix: string;
    private _nextToken: string | undefined;
    protected _doneSlicing = false;

    constructor(client: S3Client, config: FileSliceConfig, logger: Logger) {
        const { path, ...sliceConfig } = config;
        const { bucket, prefix } = parsePath(path);
        this.sliceConfig = sliceConfig;
        this.logger = logger;
        this.client = client;
        this.bucket = bucket;
        this.prefix = prefix;
    }

    private async getObjects(): Promise<FileSlice[]> {
        // const data = await listS3Objects(this.client, {
        //     Bucket: this.bucket,
        //     Prefix: this.prefix,
        //     ContinuationToken: this._nextToken
        // });

        const data = await s3RequestWithRetry(
            this.client,
            listS3Objects,
            {
                Bucket: this.bucket,
                Prefix: this.prefix,
                ContinuationToken: this._nextToken
            }
        ) as S3ClientResponse.ListObjectsV2Output;

        if (!data.Contents?.length) {
            // Returning an empty array will signal to the slicer that it is done
            // TODO: log a message to let the user know there weren't any slices
            return [];
        }

        this._nextToken = data.NextContinuationToken;

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
