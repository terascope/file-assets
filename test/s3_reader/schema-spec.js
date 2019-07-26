'use strict';

const { TestContext } = require('@terascope/job-components');
const Schema = require('../../asset/s3_reader/schema');

describe('S3 exporter Schema', () => {
    const context = new TestContext('s3-reader');
    const schema = new Schema(context);

    afterAll(() => {
        context.apis.foundation.getSystemEvents().removeAllListeners();
    });

    describe('when validating the schema', () => {
        it('should throw an error if no bucket is specified', () => {
            expect(() => {
                schema.validate({
                    _op: 's3_reader'
                });
            }).toThrowError(/Validation failed for operation config: s3_reader - bucket: This field is required and must by of type string/);
        });
        it('should throw an error if no connection is specified', () => {
            expect(() => {
                schema.validate({
                    _op: 's3_reader',
                    bucket: 'chillywilly'
                });
            }).toThrowError(/Validation failed for operation config: s3_reader - connection: This field is required and must by of type string/);
        });

        it('should not throw an error if valid config is given', () => {
            expect(() => {
                schema.validate({
                    _op: 's3_reader',
                    bucket: 'chillywilly',
                    connection: 'my-s3-connector'
                });
            }).not.toThrowError();
        });
    });
});