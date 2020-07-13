import {
    Slicer,
    WorkerContext,
    ExecutionConfig,
    TSError,
    flatten,
    SlicerRecoveryData
} from '@terascope/job-components';
import path from 'path';
import { HDFSReaderConfig } from './interfaces';
import { SliceConfig, SlicedFileResults } from '../__lib/interfaces';
import { sliceFile } from '../__lib/slice';
import { HDFSReaderFactoryAPI } from '../hdfs_reader_api/interfaces';
import HDFSReader from '../hdfs_reader_api/reader';

export default class HDFSFileSlicer extends Slicer<HDFSReaderConfig> {
    api!: HDFSReader;
    directories: string[];
    _doneSlicing = false;
    sliceConfig: SliceConfig;

    constructor(context: WorkerContext, opConfig: HDFSReaderConfig, exConfig: ExecutionConfig) {
        super(context, opConfig, exConfig);
        this.sliceConfig = Object.assign({}, opConfig);
        this.directories = [opConfig.path];
    }

    /**
     * Currently only enable autorecover jobs
     *
     * @todo we should probably support full recovery
    */
    isRecoverable(): boolean {
        return Boolean(this.executionConfig.autorecover);
    }

    async initialize(recoveryData: SlicerRecoveryData[]): Promise<void> {
        await super.initialize(recoveryData);

        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<HDFSReaderFactoryAPI>(apiName);
        // FIXME: remove as any
        this.api = await apiManager.create(apiName, {} as any);
    }

    searchFiles(metadata: Record<string, any>, filePath: string): SlicedFileResults[] {
        let fileSlices: SlicedFileResults[] = [];
        const fullPath = path.join(filePath, metadata.pathSuffix);

        if (metadata.type === 'FILE') {
            fileSlices = sliceFile({
                size: metadata.length,
                path: fullPath
            }, this.opConfig);
        } else if (metadata.type === 'DIRECTORY') {
            this.directories.push(fullPath);
        }

        return fileSlices;
    }

    async getFilePaths(filePath: string): Promise<SlicedFileResults[]> {
        let slices: SlicedFileResults[] = [];

        try {
            const dirContents: any[] = await this.api.client.listStatusAsync(filePath);
            slices = flatten(
                dirContents.map((meta: any) => this.searchFiles(meta, filePath))
            );
        } catch (err) {
            // Catch the error and log it so the execution controller doesn't crash and burn if
            // there is a bad file or directory
            const hdfsError = new TSError(err, {
                reason: 'Error while gathering slices',
                context: {
                    filePath
                }
            });
            this.logger.error(hdfsError);
        }

        if (slices.length === 0) return this.getFilePaths(this.directories.shift() as string);
        return slices;
    }

    async slice(): Promise<any[]> {
        if (this.directories.length > 0) {
            return this.getFilePaths(this.directories.shift() as string);
        }
        return [];
    }
}
