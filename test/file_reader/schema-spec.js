'use strict';

const { TestContext } = require('@terascope/job-components');
const Schema = require('../../asset/file_reader/schema');

describe('S3 exporter Schema', () => {
    const context = new TestContext('s3-reader');
    const schema = new Schema(context);

    afterAll(() => {
        context.apis.foundation.getSystemEvents().removeAllListeners();
    });

    describe('when validating the schema', () => {
        it('should throw an error if no path is specified', () => {
            expect(() => {
                schema.validate({
                    _op: 'file_reader'
                });
            }).toThrowError(/Validation failed for operation config: file_reader - path: This field is required and must by of type string/);
        });
    });
});
