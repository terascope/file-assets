'use strict';

const {
    BatchProcessor
} = require('@terascope/job-components');
const Promise = require('bluebird');
const stringHash = require('string-hash');
const path = require('path');

class PartitionByDate extends BatchProcessor {
    addPath(record, opConfig) {
        let hashString = '';
        opConfig.fields.forEach((field) => {
            hashString += `${record[field]}`;
        });
        const partition = stringHash(hashString) % opConfig.partitions;
        record.setMetadata(
            'file:partition',
            path.join(
                opConfig.path,
                `partition=${partition}/`
            )
        );

        return record;
    }

    async onBatch(slice) {
        return Promise.all(slice.map((record) => this.addPath(record, this.opConfig)));
    }
}

module.exports = PartitionByDate;
