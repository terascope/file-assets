"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const job_components_1 = require("@terascope/job-components");
const utils_1 = require("@terascope/utils");
const path_1 = __importDefault(require("path"));
const fs_extra_1 = __importDefault(require("fs-extra"));
const slice_1 = require("../__lib/slice");
class FileSlicer extends job_components_1.Slicer {
    constructor(context, opConfig, executionConfig) {
        super(context, opConfig, executionConfig);
        this._doneSlicing = false;
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
            const dirStats = fs_extra_1.default.lstatSync(this.opConfig.path);
            if (dirStats.isSymbolicLink()) {
                const error = new utils_1.TSError({ reason: `Directory '${this.opConfig.path}' cannot be a symlink!` });
                throw error;
            }
            const dirContents = fs_extra_1.default.readdirSync(this.opConfig.path);
            if (dirContents.length === 0) {
                const error = new utils_1.TSError({ reason: `Directory '${this.opConfig.path}' must not be empty!` });
                throw error;
            }
        }
        catch (err) {
            const error = new utils_1.TSError(err, { reason: 'Path must be valid!' });
            throw error;
        }
    }
    async getPath(filePath, file) {
        const fullPath = path_1.default.join(filePath, file);
        const stats = await fs_extra_1.default.lstat(fullPath);
        let fileSlices = [];
        if (stats.isFile()) {
            const fileInfo = await fs_extra_1.default.stat(fullPath);
            fileSlices = slice_1.sliceFile({ size: fileInfo.size, path: fullPath }, this.sliceConfig);
        }
        else if (stats.isDirectory()) {
            this.directories.push(fullPath);
        }
        else {
            const error = new utils_1.TSError({ reason: `${file} is not a file or directory!!` });
            this.logger.error(error);
        }
        return fileSlices;
    }
    async getFilePaths(filePath) {
        const dirContents = await fs_extra_1.default.readdir(filePath);
        let slices = [];
        try {
            const actions = [];
            // Slice whatever objects are returned from the query
            for (const file of dirContents) {
                actions.push(this.getPath(filePath, file));
            }
            const results = await Promise.all(actions);
            slices = utils_1.flatten(results);
        }
        catch (err) {
            // Catch the error and log it so the execution controller doesn't crash and burn if
            // there is a bad file or directory
            const error = new utils_1.TSError(err, {
                reason: 'Error while gathering slices',
                context: {
                    filePath
                }
            });
            this.logger.error(error);
        }
        // TODO: what if this is undefined
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