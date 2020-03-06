import {
    Slicer, getClient, WorkerContext, ExecutionConfig
} from '@terascope/job-components';
import { flatten } from '@terascope/utils';
import { S3ReaderConfig } from './interfaces';
// import { getOffsets } from '@terascope/chunked-file-reader';
import { sliceFile } from '../__lib/slice';
import { parsePath } from '../__lib/fileName';
import { SliceConfig } from '../__lib/interfaces';

export default class S3Slicer extends Slicer<S3ReaderConfig> {
    client: any;
    bucket: string;
    prefix: string;
    _doneSlicing = false;
    _lastKey: string | undefined;
    sliceConfig: SliceConfig;

    constructor(context: WorkerContext, opConfig: S3ReaderConfig, exConfig: ExecutionConfig) {
        super(context, opConfig, exConfig);
        this.client = getClient(context, opConfig, 's3');
        const { bucket, prefix } = parsePath(opConfig.path);
        this.bucket = bucket;
        this.prefix = prefix;
        this.sliceConfig = Object.assign({}, opConfig);
        if (this.opConfig.compression !== 'none') this.sliceConfig.file_per_slice = true;
    }

    /**
     * Currently only enable autorecover jobs
     *
     * @todo we should probably support full recovery
    */
    isRecoverable() {
        return Boolean(this.executionConfig.autorecover);
    }

    async slice() {
        // First check to see if there are more objects in S3
        if (this._doneSlicing) return null;

        // Get an array of slices
        const slices = await this.getObjects();

        // Finish slicer if there are no slices.
        if (slices.length === 0) return null;

        return slices;
    }

    async getObjects() {
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
            const file = {
                path: content.Key,
                size: content.Size
            };
            actions.push(sliceFile(file, this.sliceConfig));
        }

        const results = await Promise.all(actions);

        return flatten(results);
    }
}
