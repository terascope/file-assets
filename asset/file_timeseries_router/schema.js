'use strict';

const { ConvictSchema } = require('@terascope/job-components');

class Schema extends ConvictSchema {
    build() {
        return {
            base_path: {
                doc: 'Optional base path for file. If not provided, the generated date string will '
                    + 'be the root directory.',
                default: '',
                format: String
            },
            prefix: {
                doc: 'Optional prefix for filenames.',
                default: '',
                format: String
            },
            date_field: {
                doc: 'Which field in each data record contains the date to use for timeseries',
                default: 'date',
                format: String
            },
            type: {
                doc: 'Type of timeseries data',
                default: 'daily',
                format: ['daily', 'monthly', 'yearly']
            }
        };
    }
}

module.exports = Schema;
