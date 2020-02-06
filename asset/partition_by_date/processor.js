'use strict';

const {
    BatchProcessor
} = require('@terascope/job-components');
const Promise = require('bluebird');
const path = require('path');

class PartitionByDate extends BatchProcessor {
    addPath(record, opConfig) {
        const offsets = {
            daily: 10,
            monthly: 7,
            yearly: 4
        };
        // This value is enforced by the schema
        const end = offsets[opConfig.resolution];
        const date = `date_year=${new Date(record[opConfig.field]).toISOString().slice(0, end)}`
            .replace('-', '/date_month=')
            .replace('-', '/date_day=');
        record.setMetadata(
            'file:partition',
            path.join(
                opConfig.path,
                date,
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

module.exports = PartitionByDate;
