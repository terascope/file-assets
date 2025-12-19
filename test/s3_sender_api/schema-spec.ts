import 'jest-extended';
import { debugLogger } from '@terascope/core-utils';
import { newTestJobConfig, WorkerTestHarness } from 'teraslice-test-harness';
import { ValidatedJobConfig, TestClientConfig } from '@terascope/job-components';
import { S3ExporterAPIConfig } from '../../asset/src/s3_sender_api/interfaces.js';

describe('S3 Sender API Schema', () => {
    const logger = debugLogger('test');
    let harness: WorkerTestHarness;

    const clientConfig: TestClientConfig = {
        type: 's3',
        config: {},
        async createClient() {
            return {
                client: {},
                logger
            };
        },
        endpoint: 'default'
    };

    const clients = [clientConfig];

    async function makeTest(apiConfig: Partial<S3ExporterAPIConfig> = {}) {
        const apiName = 's3_sender_api';

        const config = Object.assign(
            { _name: apiName },
            apiConfig
        );

        const testJob: Partial<ValidatedJobConfig> = {
            analytics: true,
            apis: [config],
            operations: [
                { _op: 's3_sender', _api_name: apiName },
                { _op: 'noop' },
            ],
        };

        const job = newTestJobConfig(testJob);

        harness = new WorkerTestHarness(job, { clients });
        await harness.initialize();
    }

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    describe('when validating the schema', () => {
        it('should throw an error if no path is specified', async () => {
            await expect(makeTest({})).toReject();
        });

        it('should throw an error if file_per_slice is set to false/undefined', async () => {
            await expect(makeTest({ path: 'some/path', file_per_slice: false })).toReject();
            await expect(makeTest({ path: 'some/path', file_per_slice: undefined })).toReject();
        });
    });
});
