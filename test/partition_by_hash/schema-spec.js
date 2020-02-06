'use strict';

const { TestContext } = require('@terascope/job-components');
const Schema = require('../../asset/partition_by_hash/schema');

describe('Hash partitioner Schema', () => {
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
            }).toThrowError(/Invalid `fields` option: must include at least one field to partition on./);
        });
        it('should throw an error if `fields` is not an array', () => {
            expect(() => {
                schema.validate({
                    _op: 'partition_by_hash',
                    fields: null
                });
            }).toThrowError(/Invalid `fields` option: must be an array./);
            expect(() => {
                schema.validate({
                    _op: 'partition_by_hash',
                    fields: undefined
                });
            }).toThrowError(/Invalid `fields` option: must be an array./);
            expect(() => {
                schema.validate({
                    _op: 'partition_by_hash',
                    fields: JSON.stringify('this ia a string')
                });
            }).toThrowError(/Invalid `fields` option: must be an array./);
            expect(() => {
                schema.validate({
                    _op: 'partition_by_hash',
                    fields: 42
                });
            }).toThrowError(/Invalid `fields` option: must be an array./);
        });
    });
});
