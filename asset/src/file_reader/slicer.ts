import {
    Slicer,
    WorkerContext,
    ExecutionConfig,
    TSError,
    flatten,
    SlicerRecoveryData
} from '@terascope/job-components';
import path from 'path';
import { FileConfig } from './interfaces';
import { SliceConfig, SlicedFileResults } from '../__lib/interfaces';
import { sliceFile } from '../__lib/slice';
import { FileReaderFactoryAPI } from '../file_reader_api/interfaces';
import FileReader from '../file_reader_api/reader';

export default class FileSlicer extends Slicer {
    directories: string[];
    _doneSlicing = false;
    sliceConfig: SliceConfig;
    api!: FileReader;

    constructor(context: WorkerContext, opConfig: FileConfig, executionConfig: ExecutionConfig) {
        super(context, opConfig, executionConfig);
        this.directories = [opConfig.path];
        this.sliceConfig = Object.assign({}, opConfig);
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
        const apiManager = this.getAPI<FileReaderFactoryAPI>(apiName);
        // FIXME: remove as any
        this.api = await apiManager.create(apiName, {} as any);

        // NOTE ORDER MATTERS
        // a parallel slicer initialize calls newSlicer multiple times
        // need to make api before newSlicer is called

        this.checkProvidedPath();
    }

    private checkProvidedPath(): void {
        try {
            const dirStats = this.api.client.lstatSync(this.opConfig.path);

            if (dirStats.isSymbolicLink()) {
                const error = new TSError({ reason: `Directory '${this.opConfig.path}' cannot be a symlink!` });
                throw error;
            }

            const dirContents = this.api.client.readdirSync(this.opConfig.path);

            if (dirContents.length === 0) {
                const error = new TSError({ reason: `Directory '${this.opConfig.path}' must not be empty!` });
                throw error;
            }
        } catch (err) {
            const error = new TSError(err, { reason: 'Path must be valid!' });
            throw error;
        }
    }

    async getPath(filePath: string, file: string): Promise<SlicedFileResults[]> {
        const fullPath = path.join(filePath, file);
        const stats = await this.api.client.lstat(fullPath);

        let fileSlices: SlicedFileResults[] = [];

        if (stats.isFile()) {
            const fileInfo = await this.api.client.stat(fullPath);
            fileSlices = sliceFile({ size: fileInfo.size, path: fullPath }, this.sliceConfig);
        } else if (stats.isDirectory()) {
            this.directories.push(fullPath);
        } else {
            const error = new TSError({ reason: `${file} is not a file or directory!!` });
            this.logger.error(error);
        }

        return fileSlices;
    }

    canReadFile(fileName: string): boolean {
        if (fileName.charAt(0) === '.') return false;
        return true;
    }

    async getFilePaths(filePath: string): Promise<SlicedFileResults[]> {
        const dirContents = await this.api.client.readdir(filePath);
        let slices: SlicedFileResults[] = [];

        try {
            const actions = [];

            // Slice whatever objects are returned from the query
            for (const file of dirContents) {
                if (this.canReadFile(file)) actions.push(this.getPath(filePath, file));
            }

            const results = await Promise.all(actions);
            slices = flatten(results);
        } catch (err) {
            // Catch the error and log it so the execution controller doesn't crash and burn if
            // there is a bad file or directory
            const error = new TSError(err, {
                reason: 'Error while gathering slices',
                context: {
                    filePath
                }
            });
            this.logger.error(error);
        }
        // TODO: what if this is undefined
        if (slices.length === 0) return this.getFilePaths(this.directories.shift() as string);
        return slices;
    }

    async slice(): Promise<any[]|null> {
        if (this.directories.length > 0) {
            return this.getFilePaths(this.directories.shift() as string);
        }
        return null;
    }
}
