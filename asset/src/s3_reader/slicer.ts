import {
    Slicer,
    WorkerContext,
    ExecutionConfig,
    flatten,
    SlicerRecoveryData
} from '@terascope/job-components';
import { S3ReaderConfig } from './interfaces';
import { parsePath } from './helpers';
import { sliceFile } from '../__lib/slice';
import { SliceConfig, SlicedFileResults } from '../__lib/interfaces';
import { S3ReaderFactoryAPI } from '../s3_reader_api/interfaces';
import S3Reader from '../s3_reader_api/reader';

export default class S3Slicer extends Slicer<S3ReaderConfig> {
    bucket: string;
    prefix: string;
    _doneSlicing = false;
    _lastKey: string | undefined;
    sliceConfig: SliceConfig;
    api!: S3Reader;

    constructor(context: WorkerContext, opConfig: S3ReaderConfig, exConfig: ExecutionConfig) {
        super(context, opConfig, exConfig);
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
    isRecoverable(): boolean {
        return Boolean(this.executionConfig.autorecover);
    }

    canReadFile(filePath: string): boolean {
        const args = filePath.split('/');
        const hasDot = args.some((segment) => segment.charAt(0) === '.');

        if (hasDot) return false;
        return true;
    }

    async initialize(recoveryData: SlicerRecoveryData[]): Promise<void> {
        await super.initialize(recoveryData);

        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<S3ReaderFactoryAPI>(apiName);
        // FIXME: remove as any
        this.api = await apiManager.create(apiName, {} as any);
    }

    async slice(): Promise<any|null> {
        // First check to see if there are more objects in S3
        if (this._doneSlicing) return null;

        // Get an array of slices
        const slices = await this.getObjects();

        // Finish slicer if there are no slices.
        if (slices.length === 0) return null;

        return slices;
    }

    async getObjects(): Promise<SlicedFileResults[]> {
        const data = await this.api.client.listObjects_Async({
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
            if (this.canReadFile(content.Key)) {
                const file = {
                    path: content.Key,
                    size: content.Size
                };
                actions.push(sliceFile(file, this.sliceConfig));
            } else {
                this.logger.warn(`Invalid path ${content.Key}, cannot start with a dot in directory of file name, skipping path`);
            }
        }

        const results = await Promise.all(actions);

        return flatten(results);
    }
}
