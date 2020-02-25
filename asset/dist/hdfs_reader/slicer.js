"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const job_components_1 = require("@terascope/job-components");
const utils_1 = require("@terascope/utils");
const path_1 = __importDefault(require("path"));
const slice_1 = require("../__lib/slice");
class FileSlicer extends job_components_1.Slicer {
    constructor(context, opConfig, exConfig) {
        super(context, opConfig, exConfig);
        this._doneSlicing = false;
        this.client = job_components_1.getClient(context, opConfig, 'hdfs_ha').client;
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
    searchFiles(metadata, filePath) {
        let fileSlices = [];
        const fullPath = path_1.default.join(filePath, metadata.pathSuffix);
        if (metadata.type === 'FILE') {
            fileSlices = slice_1.sliceFile({
                size: metadata.length,
                path: fullPath
            }, this.opConfig);
        }
        else if (metadata.type === 'DIRECTORY') {
            this.directories.push(fullPath);
        }
        return fileSlices;
    }
    async getFilePaths(filePath) {
        let slices = [];
        try {
            const dirContents = await this.client.listStatusAsync(filePath);
            slices = utils_1.flatten(dirContents.map((meta) => this.searchFiles(meta, filePath)));
        }
        catch (err) {
            // Catch the error and log it so the execution controller doesn't crash and burn if
            // there is a bad file or directory
            const hdfsError = new utils_1.TSError(err, {
                reason: 'Error while gathering slices',
                context: {
                    filePath
                }
            });
            this.logger.error(hdfsError);
        }
        if (slices.length === 0)
            return this.getFilePaths(this.directories.shift());
        return slices;
    }
    async slice() {
        if (this.directories.length > 0) {
            return this.getFilePaths(this.directories.shift());
        }
        return [];
    }
}
exports.default = FileSlicer;
//# sourceMappingURL=slicer.js.map