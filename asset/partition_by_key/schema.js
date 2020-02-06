'use strict';

const { ConvictSchema } = require('@terascope/job-components');

class Schema extends ConvictSchema {
    build() {
        return {
            path: {
                doc: 'Optional base path for file. If not provided, the generated key string will '
                    + 'be the root directory.',
                default: '',
                format: String
            }
        };
    }
}

module.exports = Schema;
