'use strict';

const { TestContext } = require('@terascope/job-components');
const Schema = require('../../asset/partition_by_fields/schema');

describe('Field partitioner Schema', () => {
    const context = new TestContext('partition-by-fields');
    const schema = new Schema(context);

    afterAll(() => {
        context.apis.foundation.getSystemEvents().removeAllListeners();
    });

    describe('when validating the schema', () => {
        it('should throw an error if no fields specified', () => {
            expect(() => {
                schema.validate({
                    _op: 'partition_by_fields'
                });
            }).toThrowError(/Invalid `fields` option: must include at least one field to partition on./);
        });
        it('should throw an error if `fields` is not an array', () => {
            expect(() => {
                schema.validate({
                    _op: 'partition_by_fields',
                    fields: null
                });
            }).toThrowError(/Invalid `fields` option: must be an array./);
            expect(() => {
                schema.validate({
                    _op: 'partition_by_fields',
                    fields: undefined
                });
            }).toThrowError(/Invalid `fields` option: must be an array./);
            expect(() => {
                schema.validate({
                    _op: 'partition_by_fields',
                    fields: JSON.stringify('this ia a string')
                });
            }).toThrowError(/Invalid `fields` option: must be an array./);
            expect(() => {
                schema.validate({
                    _op: 'partition_by_fields',
                    fields: 42
                });
            }).toThrowError(/Invalid `fields` option: must be an array./);
        });
    });
});
