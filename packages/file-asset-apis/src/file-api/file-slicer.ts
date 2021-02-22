import {
    Logger,
    TSError,
    flatten,
} from '@terascope/job-components';
import fse from 'fs-extra';
import path from 'path';
import { segmentFile, canReadFile } from '../base/slice';
import { SliceConfig, SlicedFileResults, FileSliceConfig } from '../interfaces';

export class FileSlicer {
    readonly directories: string[];
    readonly sliceConfig: SliceConfig;
    logger: Logger;

    constructor(config: FileSliceConfig, logger: Logger) {
        this.directories = [config.path];
        this.sliceConfig = Object.assign({}, config);
        this.logger = logger;
    }

    private async getPath(filePath: string, file: string): Promise<SlicedFileResults[]> {
        const fullPath = path.join(filePath, file);
        const stats = await fse.lstat(fullPath);

        let fileSlices: SlicedFileResults[] = [];

        if (stats.isFile()) {
            const fileInfo = await fse.stat(fullPath);
            fileSlices = segmentFile({ size: fileInfo.size, path: fullPath }, this.sliceConfig);
        } else if (stats.isDirectory()) {
            this.directories.push(fullPath);
        } else {
            const error = new TSError({ reason: `${file} is not a file or directory!!` });
            this.logger.error(error);
        }

        return fileSlices;
    }

    private async getFilePaths(filePath: string): Promise<SlicedFileResults[]> {
        const dirContents = await fse.readdir(filePath);
        let slices: SlicedFileResults[] = [];

        try {
            const actions = [];

            // Slice whatever objects are returned from the query
            for (const file of dirContents) {
                if (canReadFile(file)) actions.push(this.getPath(filePath, file));
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

        if (slices.length === 0) return this.getFilePaths(this.directories.shift() as string);
        return slices;
    }

    /**
    * This method will return an array of file slices, or null if the slicer is done
    */
    async slice(): Promise<SlicedFileResults[]|null> {
        if (this.directories.length > 0) {
            return this.getFilePaths(this.directories.shift() as string);
        }
        return null;
    }
}
