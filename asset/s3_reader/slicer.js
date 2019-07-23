'use strict';

const { Slicer, getClient } = require('@terascope/job-components');
const Queue = require('@terascope/queue');
const { getOffsets } = require('@terascope/chunked-file-reader');

class S3Slicer extends Slicer {
    constructor(context, opConfig, executionConfig) {
        super(context, opConfig, executionConfig);
        this.client = getClient(context, opConfig, 's3');
        this._lastKey = undefined;
        this._queue = new Queue();
        this._doneSlicing = false;
    }

    async initialize() {
        this.getObjects();
    }

    async slice() {
        // this.getObjects();
        // return [() => {
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
        // }];
    }

    async getObjects() {
        const data = await this.client.listObjects_Async({
            Bucket: this.opConfig.bucket,
            Prefix: this.opConfig.prefix,
            Marker: this._lastKey,
        });

        // console.log('heyo lmao');
        // console.log(data)
        // console.log(data.Contents)
        // console.log(data.Contents.length)
        // console.log(data.Contents[data.Contents.length - 1])

        this._lastKey = data.Contents[data.Contents.length - 1].Key;

        // Always slice whatever objects are returned from the query
        this.sliceObjects(data.Contents);
        if (data.IsTruncated) {
            // Continue slicing objects if there are more to slice
            this._doneSlicing = false;
            this.getObjects();
        } else {
            this._doneSlicing = true;
        }
    }

    async sliceObjects(objList) {
        objList.forEach((obj) => {
            if (this.opConfig.format === 'json') {
                const offset = {
                    path: obj.Key,
                    offset: 0,
                    length: obj.Size,
                };
                this._queue.enqueue(offset);
            } else {
                getOffsets(
                    this.opConfig.size,
                    obj.Size,
                    this.opConfig.line_delimiter
                ).forEach((offset) => {
                    offset.path = obj.Key;
                    offset.total = obj.Size;
                    this._queue.enqueue(offset);
                });
            }
        });
    }
}

module.exports = S3Slicer;
