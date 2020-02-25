"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const job_components_1 = require("@terascope/job-components");
const utils_1 = require("@terascope/utils");
// import { getOffsets } from '@terascope/chunked-file-reader';
const slice_1 = require("../__lib/slice");
const fileName_1 = require("../__lib/fileName");
class S3Slicer extends job_components_1.Slicer {
    constructor(context, opConfig, exConfig) {
        super(context, opConfig, exConfig);
        this._doneSlicing = false;
        this.client = job_components_1.getClient(context, opConfig, 's3');
        const { bucket, prefix } = fileName_1.parsePath(opConfig.path);
        this.bucket = bucket;
        this.prefix = prefix;
        this.sliceConfig = Object.assign({}, opConfig);
        if (this.opConfig.compression !== 'none')
            this.sliceConfig.file_per_slice = true;
    }
    /**
     * Currently only enable autorecover jobs
     *
     * @todo we should probably support full recovery
    */
    isRecoverable() {
        return Boolean(this.executionConfig.autorecover);
    }
    async slice() {
        // First check to see if there are more objects in S3
        if (this._doneSlicing)
            return null;
        // Get an array of slices
        const slices = await this.getObjects();
        // Finish slicer if there are no slices.
        if (slices.length === 0)
            return null;
        return slices;
    }
    async getObjects() {
        const data = await this.client.listObjects_Async({
            Bucket: this.bucket,
            Prefix: this.prefix,
            Marker: this._lastKey,
        });
        if (data.Contents.length === 0) {
            // Returning an empty array will signal to the slicer that it is done
            // TODO: log a message to let the user know there weren't any slices
            return [];
        }
        this._lastKey = data.Contents[data.Contents.length - 1].Key;
        // Let slicer know whether or not there are more objects to process
        if (data.IsTruncated) {
            this._doneSlicing = false;
        }
        else {
            this._doneSlicing = true;
        }
        const actions = [];
        // Slice whatever objects are returned from the query
        for (const content of data.Contents) {
            const file = {
                path: content.Key,
                size: content.Size
            };
            actions.push(slice_1.sliceFile(file, this.sliceConfig));
        }
        const results = await Promise.all(actions);
        return utils_1.flatten(results);
    }
}
exports.default = S3Slicer;
//# sourceMappingURL=slicer.js.map