'use strict';

const {
    BatchProcessor
} = require('@terascope/job-components');
const path = require('path');

class PartitionByDate extends BatchProcessor {
    addPath(record, opConfig) {
        const partitions = [];
        const offsets = {
            daily: 10,
            monthly: 7,
            yearly: 4
        };
        // This value is enforced by the schema
        const end = offsets[opConfig.resolution];
        const dates = new Date(record[opConfig.field]).toISOString().slice(0, end).split('-');
        // Schema enforces one of these formatting options
        if (opConfig.resolution === 'yearly') {
            partitions.push(`date_year=${dates[0]}`);
        } else if (opConfig.resolution === 'monthly') {
            partitions.push(`date_year=${dates[0]}`);
            partitions.push(`date_month=${dates[1]}`);
        } else {
            partitions.push(`date_year=${dates[0]}`);
            partitions.push(`date_month=${dates[1]}`);
            partitions.push(`date_day=${dates[2]}`);
        }
        record.setMetadata(
            'file:partition',
            path.join(
                opConfig.path,
                ...partitions,
                // Guarantees the date won't end up as a filename prefix
                '/'
            )
        );

        return record;
    }

    async onBatch(slice) {
        return slice.map((record) => this.addPath(record, this.opConfig));
    }
}

module.exports = PartitionByDate;
