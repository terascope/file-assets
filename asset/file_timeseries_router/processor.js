'use strict';

const {
    BatchProcessor
} = require('@terascope/job-components');
const Promise = require('bluebird');
const path = require('path');

class TimeseriesRouter extends BatchProcessor {
    addPath(record, opConfig) {
        const offsets = {
            daily: 10,
            monthly: 7,
            yearly: 4
        };

        // This value is enforced by the schema
        const end = offsets[opConfig.timeseries];
        const date = new Date(record[opConfig.date_field]).toISOString().slice(0, end);
        record.setMetadata(
            'routingPath',
            path.join(
                opConfig.base_path,
                date.replace(/-/gi, '.'),
                // Guarantees the date won't end up as a filename prefix
                '/',
                opConfig.prefix
            )
        );

        return record;
    }

    async onBatch(slice) {
        return Promise.all(slice, (record) => this.addPath(record));
    }
}

module.exports = TimeseriesRouter;
