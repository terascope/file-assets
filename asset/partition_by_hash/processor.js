'use strict';

const {
    BatchProcessor
} = require('@terascope/job-components');
const fnv1a = require('@sindresorhus/fnv1a');
const path = require('path');

class PartitionByDate extends BatchProcessor {
    addPath(record, opConfig) {
        let hashString = '';
        opConfig.fields.forEach((field) => {
            hashString += `${record[field]}`;
        });
        const partition = fnv1a(hashString) % opConfig.partitions;
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
        return slice.map((record) => this.addPath(record, this.opConfig));
    }
}

module.exports = PartitionByDate;
