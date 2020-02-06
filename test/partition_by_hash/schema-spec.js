'use strict';

const { TestContext } = require('@terascope/job-components');
const Schema = require('../../asset/partition_by_hash/schema');

describe('S3 exporter Schema', () => {
    const context = new TestContext('partition-by-hash');
    const schema = new Schema(context);

    afterAll(() => {
        context.apis.foundation.getSystemEvents().removeAllListeners();
    });

    describe('when validating the schema', () => {
        it('should throw an error if no fields specified', () => {
            expect(() => {
                schema.validate({
                    _op: 'partition_by_hash'
                });
            }).toThrowError(/Must include at least one field to hash for partitioning!/);
        });
    });
});
