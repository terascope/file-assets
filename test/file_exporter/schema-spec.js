'use strict';

const { TestContext } = require('@terascope/job-components');
const Schema = require('../../asset/file_exporter/schema');

describe('File exporter Schema', () => {
    const context = new TestContext('file-exporter');
    const schema = new Schema(context);

    afterAll(() => {
        context.apis.foundation.getSystemEvents().removeAllListeners();
    });

    describe('when validating the schema', () => {
        it('should throw an error if no path is specified', () => {
            expect(() => {
                schema.validate({
                    _op: 'file_exporter'
                });
            }).toThrowError(/Validation failed for operation config: file_exporter - path: This field is required and must by of type string/);
        });

        it('should not throw an error if valid config is given', () => {
            expect(() => {
                schema.validate({
                    _op: 'file_exporter',
                    path: '/chillywilly'
                });
            }).not.toThrowError();
        });
    });
});
