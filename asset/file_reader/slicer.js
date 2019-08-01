'use strict';

const { Slicer } = require('@terascope/job-components');
const Queue = require('@terascope/queue');
const { getOffsets } = require('@terascope/chunked-file-reader');
const { TSError } = require('@terascope/utils');
const Promise = require('bluebird');
const path = require('path');
const fse = require('fs-extra');

class FileSlicer extends Slicer {
    constructor(context, opConfig, executionConfig) {
        super(context, opConfig, executionConfig);
        this.client = fse;
        this._queue = new Queue();
        this._doneSlicing = false;
    }

    async initialize() {
        this.checkProvidedPath(this.opConfig.path);
        this.getFilePaths(this.opConfig.path)
            .then(() => {
                this._doneSlicing = true;
            });
    }

    checkProvidedPath(filePath) {
        try {
            const dirStats = this.client.lstatSync(filePath);
            if (dirStats.isSymbolicLink()) {
                throw new Error('Directory for `file_reader` cannot be a symlink!');
            }
            const dirContents = this.client.readdirSync(filePath);
            if (dirContents.length === 0) {
                throw new Error('Must provide a non-empty directory for `file_reader`!!');
            }
        } catch (err) {
            throw new Error(`Path must be valid! Encountered the following error:\n${err}`);
        }
    }

    async slice() {
        // Grab a record if there is one ready in the queue
        if (this._queue.size() > 0) return this._queue.dequeue();

        // Finish slicer if the queue is empty and it's done prepping slices
        if (this._doneSlicing) return null;

        // If the queue is empty and there are still slices, wait for a new slice
        return new Promise((resolve) => {
            const intervalId = setInterval(() => {
                if (this._queue.size() > 0) {
                    clearInterval(intervalId);
                    resolve(this._queue.dequeue());
                }
            }, 50);
        });
    }

    async getFilePaths(filePath) {
        const dirContents = await this.client.readdir(filePath);
        return Promise.map(dirContents, async (file) => {
            const fullPath = path.join(filePath, file);
            const stats = await this.client.lstat(fullPath);
            if (stats.isFile()) {
                return this.processFile(fullPath);
            }
            if (stats.isDirectory()) {
                return this.getFilePaths(fullPath);
            }
            this.logger.error(`${file} is not a file or directory!!`);
            return true;
        })
            // Catch the error and log it so the execution controller doesn't crash and burn if
            // there is a bad file or directory
            .catch((err) => {
                const error = new TSError(err, {
                    reason: 'Error while reading file or directory',
                    context: {
                        filePath
                    }
                });
                this.logger.error(error);
            });
    }

    async processFile(filePath) {
        const stat = await this.client.stat(filePath);
        const total = stat.size;
        // Override file slicing to make sure JSON files are not split across slices
        if (this.opConfig.format === 'json') {
            const offset = {
                path: filePath,
                offset: 0,
                length: total
            };
            this._queue.enqueue(offset);
        } else {
            getOffsets(
                this.opConfig.size,
                total,
                this.opConfig.line_delimiter
            ).forEach((offset) => {
                offset.path = filePath;
                this._queue.enqueue(offset);
            });
        }
        return true;
    }
}

module.exports = FileSlicer;
