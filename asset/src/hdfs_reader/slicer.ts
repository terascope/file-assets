import {
    Slicer,
    TSError,
    flatten,
    SlicerRecoveryData
} from '@terascope/job-components';
import path from 'path';
import {
    SliceConfig, FileSlice, segmentFile, HDFSReader
} from '@terascope/file-asset-apis';
import { HDFSReaderOpConfig } from './interfaces';
import { HDFSReaderFactoryAPI, HDFSReaderApiConfig } from '../hdfs_reader_api/interfaces';

export default class HDFSFileSlicer extends Slicer<HDFSReaderOpConfig> {
    api!: HDFSReader;
    directories!: string[];
    sliceConfig!: SliceConfig;
    _doneSlicing = false;

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
        const apiConfig = apiManager.getConfig(apiName) as HDFSReaderApiConfig;

        this.sliceConfig = Object.assign({}, apiConfig);
        this.directories = [apiConfig.path];
        this.api = await apiManager.create(apiName, {});
    }

    searchFiles(metadata: Record<string, any>, filePath: string): FileSlice[] {
        let fileSlices: FileSlice[] = [];
        const fullPath = path.join(filePath, metadata.pathSuffix);

        if (metadata.type === 'FILE') {
            fileSlices = segmentFile({
                size: metadata.length,
                path: fullPath
            }, this.sliceConfig);
        } else if (metadata.type === 'DIRECTORY') {
            this.directories.push(fullPath);
        }

        return fileSlices;
    }

    async getFilePaths(filePath: string): Promise<FileSlice[]> {
        let slices: FileSlice[] = [];

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
