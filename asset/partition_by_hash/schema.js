'use strict';

const { ConvictSchema } = require('@terascope/job-components');

class Schema extends ConvictSchema {
    build() {
        return {
            path: {
                doc: 'Optional base path for file. If not provided, the generated date string will '
                    + 'be the root directory.',
                default: '',
                format: String
            },
            fields: {
                doc: 'Specifies fields to hash for partitioning. Must specify at least one field!',
                default: [],
                format: (fields) => {
                    if (fields.length === 0) {
                        throw new Error('Must include at least one field to hash for partitioning!');
                    }
                }
            },
            partitions: {
                doc: 'Number of partitions to use with hashing',
                default: null,
                format: Number
            }
        };
    }
}

module.exports = Schema;
