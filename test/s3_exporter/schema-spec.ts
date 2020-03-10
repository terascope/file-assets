import { TestContext } from '@terascope/job-components';
import Schema from '../../asset/src/s3_exporter/schema';

describe('S3 exporter Schema', () => {
    const context = new TestContext('s3-exporter');
    const schema = new Schema(context);

    afterAll(() => {
        context.apis.foundation.getSystemEvents().removeAllListeners();
    });

    describe('when validating the schema', () => {
        it('should throw an error if no path is specified', () => {
            expect(() => {
                schema.validate({
                    _op: 's3_exporter'
                });
            }).toThrowError();
        });

        it('should not throw an error if valid config is given', () => {
            expect(() => {
                schema.validate({
                    _op: 's3_exporter',
                    path: 'chillywilly',
                    connection: 'my-s3-connector'
                });
            }).not.toThrowError();
        });
    });
});
