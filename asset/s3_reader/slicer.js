'use strict';

const { Slicer, getClient } = require('@terascope/job-components');
// const { getOffsets } = require('@terascope/chunked-file-reader');
const Promise = require('bluebird');
const { sliceFile } = require('../_lib/slice');
const { parsePath } = require('../_lib/fileName');

class S3Slicer extends Slicer {
    constructor(context, opConfig, executionConfig) {
        super(context, opConfig, executionConfig);
        this.client = getClient(context, opConfig, 's3');
        const objPath = parsePath(opConfig.path);
        this.bucket = objPath.bucket;
        this.prefix = objPath.prefix;
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
        } else {
            this._doneSlicing = true;
        }

        // Slice whatever objects are returned from the query
        // return this.sliceObjects(data.Contents);
        return Promise.map(data.Contents, (obj) => {
            const file = {
                path: obj.Key,
                size: obj.Size
            };
            return sliceFile(file, this.opConfig);
        })
            .then((slices) => [].concat(...slices));
    }
}

module.exports = S3Slicer;
