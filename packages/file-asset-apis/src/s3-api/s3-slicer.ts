import { flatten, AnyObject, Logger } from '@terascope/job-components';
import { segmentFile, parsePath, canReadFile } from '../base';
import { SliceConfig, SlicedFileResults, FileSliceConfig } from '../interfaces';

export class S3Slicer {
    readonly sliceConfig: SliceConfig;
    logger: Logger;
    client: AnyObject;
    readonly bucket: string;
    readonly prefix: string;
    _lastKey: string | undefined;
    protected _doneSlicing = false;

    constructor(client: AnyObject, config: FileSliceConfig, logger: Logger) {
        const { path, ...sliceConfig } = config;
        const { bucket, prefix } = parsePath(path);
        this.sliceConfig = sliceConfig;
        this.logger = logger;
        this.client = client;
        this.bucket = bucket;
        this.prefix = prefix;
    }

    private async getObjects(): Promise<SlicedFileResults[]> {
        const data = await this.client.listObjects_Async({
            Bucket: this.bucket,
            Prefix: this.prefix,
            Marker: this._lastKey,
        });

        if (data.Contents.length === 0) {
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
            if (canReadFile(content.Key)) {
                const file = {
                    path: content.Key,
                    size: content.Size
                };
                actions.push(segmentFile(file, this.sliceConfig));
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
    async slice(): Promise<any|null> {
        // First check to see if there are more objects in S3
        if (this._doneSlicing) return null;

        // Get an array of slices
        const slices = await this.getObjects();

        // Finish slicer if there are no slices.
        if (slices.length === 0) return null;

        return slices;
    }
}
