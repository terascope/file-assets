'use strict';

const { Slicer, getClient } = require('@terascope/job-components');
const { getOffsets } = require('@terascope/chunked-file-reader');

class S3Slicer extends Slicer {
    constructor(context, opConfig, executionConfig) {
        super(context, opConfig, executionConfig);
        this.client = getClient(context, opConfig, 's3');
        this._lastKey = undefined;
        this._doneSlicing = false;
        if (this.opConfig.compression !== 'none') this.opConfig.file_per_slice = true;
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
        if (this._doneSlicing) return null;

        // Get an array of slices
        const slices = await this.getObjects();

        // Finish slicer if there are no slices.
        if (slices.length === 0) return null;

        return slices;
    }

    async getObjects() {
        const data = await this.client.listObjects_Async({
            Bucket: this.opConfig.bucket,
            Prefix: this.opConfig.object_prefix,
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
        } else {
            this._doneSlicing = true;
        }

        // Slice whatever objects are returned from the query
        return this.sliceObjects(data.Contents);
    }

    sliceObjects(objList) {
        const slices = [];
        objList.forEach((obj) => {
            // If compression is used on the objects, `file_per_slice` will be coerced to true
            if (this.opConfig.format === 'json' || this.opConfig.file_per_slice) {
                const offset = {
                    path: obj.Key,
                    offset: 0,
                    length: obj.Size,
                };
                slices.push(offset);
            } else {
                getOffsets(
                    this.opConfig.size,
                    obj.Size,
                    this.opConfig.line_delimiter
                ).forEach((offset) => {
                    offset.path = obj.Key;
                    offset.total = obj.Size;
                    slices.push(offset);
                });
            }
        });
        return slices;
    }
}

module.exports = S3Slicer;
