import {
    Slicer, getClient, WorkerContext, ExecutionConfig
} from '@terascope/job-components';
import { TSError, flatten } from '@terascope/utils';
import path from 'path';
import { HDFSReaderConfig } from './interfaces';
import { sliceFile, SlicedFileResults, SliceConfig } from '../__lib/slice';

export default class FileSlicer extends Slicer<HDFSReaderConfig> {
    client: any;
    directories: string[];
    _doneSlicing = false;
    sliceConfig: SliceConfig;

    constructor(context: WorkerContext, opConfig: HDFSReaderConfig, exConfig: ExecutionConfig) {
        super(context, opConfig, exConfig);
        this.client = getClient(context, opConfig, 'hdfs_ha').client;
        this.sliceConfig = Object.assign({}, opConfig);
        this.directories = [opConfig.path];
    }

    /**
     * Currently only enable autorecover jobs
     *
     * @todo we should probably support full recovery
    */
    isRecoverable() {
        return Boolean(this.executionConfig.autorecover);
    }

    searchFiles(metadata: any, filePath: string) {
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
            const dirContents: any[] = await this.client.listStatusAsync(filePath);
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

    async slice() {
        if (this.directories.length > 0) {
            return this.getFilePaths(this.directories.shift() as string);
        }
        return [];
    }
}
