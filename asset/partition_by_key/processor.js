'use strict';

const {
    BatchProcessor
} = require('@terascope/job-components');
const path = require('path');

class PartitionByKey extends BatchProcessor {
    addPath(record, opConfig) {
        const key = `_key=${record.getKey()}`;
        record.setMetadata(
            'file:partition',
            path.join(
                opConfig.path,
                key,
                // Guarantees the date won't end up as a filename prefix
                '/'
            )
        );

        return record;
    }

    async onBatch(slice) {
        return Promise.all(slice.map((record) => this.addPath(record, this.opConfig)));
    }
}

module.exports = PartitionByKey;
