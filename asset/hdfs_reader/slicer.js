'use strict';

const { Slicer, getClient } = require('@terascope/job-components');
const { TSError } = require('@terascope/utils');
const Promise = require('bluebird');
const path = require('path');
const { sliceFile } = require('../_lib/slice');

class FileSlicer extends Slicer {
    constructor(context, opConfig, executionConfig) {
        super(context, opConfig, executionConfig);
        this.client = getClient(context, opConfig, 'hdfs_ha').client;
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

    async shutdown() {
        this._shutdown = true;
        return super.shutdown();
    }

    async slice() {
        if (this.directories.length > 0) return this.getFilePaths(this.directories.shift());
        return [];
    }

    async getFilePaths(filePath) {
        const dirContents = await this.client.listStatusAsync(filePath);
        // const slices = [];
        const slices = await [].concat(...await Promise.all(dirContents, async (metadata) => {
            let fileSlices = [];
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
