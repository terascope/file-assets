'use strict';

const {
    BatchProcessor
} = require('@terascope/job-components');
const Promise = require('bluebird');
const path = require('path');

class PartitionByFields extends BatchProcessor {
    addPath(record, opConfig) {
        let partition = '';
        opConfig.fields.forEach((field) => {
            partition += `${field}=${record[field]}/`;
        });
        record.setMetadata(
            'file:partition',
            path.join(
                opConfig.path,
                partition
            )
        );

        return record;
    }

    async onBatch(slice) {
        return Promise.all(slice.map((record) => this.addPath(record, this.opConfig)));
    }
}

module.exports = PartitionByFields;
