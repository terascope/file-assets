'use strict';

const { Slicer } = require('@terascope/job-components');
const { TSError } = require('@terascope/utils');
const Promise = require('bluebird');
const path = require('path');
const fse = require('fs-extra');
const { sliceFile } = require('../_lib/slice');

class FileSlicer extends Slicer {
    constructor(context, opConfig, executionConfig) {
        super(context, opConfig, executionConfig);
        this.directories = [opConfig.path];
        this._doneSlicing = false;
    }

    /**
     * Currently only enable autorecover jobs
     *
     * @todo we should probably support full recovery
    */
    isRecoverable() {
        return Boolean(this.executionConfig.autorecover);
    }

    async initialize() {
        await this.checkProvidedPath();
    }

    async shutdown() {
        this._shutdown = true;
        return super.shutdown();
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

    async slice() {
        if (this.directories.length > 0) return this.getFilePaths(this.directories.shift());
        return this.directories;
    }

    async getFilePaths(filePath) {
        const dirContents = await fse.readdir(filePath);
        // const slices = [];
        const slices = await [].concat(...await Promise.map(dirContents, async (file) => {
            let fileSlices = [];
            const fullPath = path.join(filePath, file);
            const stats = await fse.lstat(fullPath);
            if (stats.isFile()) {
                await fse.stat(fullPath)
                    .then((fileInfo) => {
                        fileSlices = sliceFile({
                            size: fileInfo.size,
                            path: fullPath
                        }, this.opConfig);
                    });
            } else if (stats.isDirectory()) {
                this.directories.push(fullPath);
            } else {
                const error = new TSError({ reason: `${file} is not a file or directory!!` });
                this.logger.error(error);
            }
            return fileSlices;
        })
            // Catch the error and log it so the execution controller doesn't crash and burn if
            // there is a bad file or directory
            .catch((err) => {
                const error = new TSError(err, {
                    reason: 'Error while gathering slices',
                    context: {
                        filePath
                    }
                });
                this.logger.error(error);
            }));
        if (slices.length === 0) return this.getFilePaths(this.directories.shift());
        return slices;
    }
}

module.exports = FileSlicer;
