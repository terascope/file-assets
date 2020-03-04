import {
    Slicer, WorkerContext, ExecutionConfig
} from '@terascope/job-components';
import { TSError, flatten } from '@terascope/utils';
import path from 'path';
import fse from 'fs-extra';
import { FileConfig } from './interfaces';
import { sliceFile, SliceConfig, SlicedFileResults } from '../__lib/slice';

export default class FileSlicer extends Slicer {
    directories: string[];
    _doneSlicing = false;
    sliceConfig: SliceConfig;

    constructor(context: WorkerContext, opConfig: FileConfig, executionConfig: ExecutionConfig) {
        super(context, opConfig, executionConfig);
        this.directories = [opConfig.path];
        this.sliceConfig = Object.assign({}, opConfig);
        this.checkProvidedPath();
    }

    /**
     * Currently only enable autorecover jobs
     *
     * @todo we should probably support full recovery
    */
    isRecoverable() {
        return Boolean(this.executionConfig.autorecover);
    }

    checkProvidedPath() {
        try {
            const dirStats = fse.lstatSync(this.opConfig.path);

            if (dirStats.isSymbolicLink()) {
                const error = new TSError({ reason: `Directory '${this.opConfig.path}' cannot be a symlink!` });
                throw error;
            }

            const dirContents = fse.readdirSync(this.opConfig.path);

            if (dirContents.length === 0) {
                const error = new TSError({ reason: `Directory '${this.opConfig.path}' must not be empty!` });
                throw error;
            }
        } catch (err) {
            const error = new TSError(err, { reason: 'Path must be valid!' });
            throw error;
        }
    }

    async getPath(filePath: string, file: string) {
        const fullPath = path.join(filePath, file);
        const stats = await fse.lstat(fullPath);

        let fileSlices: SlicedFileResults[] = [];

        if (stats.isFile()) {
            const fileInfo = await fse.stat(fullPath);
            fileSlices = sliceFile({ size: fileInfo.size, path: fullPath }, this.sliceConfig);
        } else if (stats.isDirectory()) {
            this.directories.push(fullPath);
        } else {
            const error = new TSError({ reason: `${file} is not a file or directory!!` });
            this.logger.error(error);
        }

        return fileSlices;
    }

    async getFilePaths(filePath: string): Promise<SlicedFileResults[]> {
        const dirContents = await fse.readdir(filePath);
        let slices: SlicedFileResults[] = [];

        try {
            const actions = [];

            // Slice whatever objects are returned from the query
            for (const file of dirContents) {
                actions.push(this.getPath(filePath, file));
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

    async slice() {
        if (this.directories.length > 0) {
            return this.getFilePaths(this.directories.shift() as string);
        }
        return [];
    }
}
