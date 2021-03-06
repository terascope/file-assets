import 'jest-extended';
import { newTestJobConfig, WorkerTestHarness } from 'teraslice-test-harness';
import { ValidatedJobConfig, TestClientConfig, Logger } from '@terascope/job-components';
import { S3ExporterAPIConfig } from '../../asset/src/s3_sender_api/interfaces';

describe('S3 Sender API Schema', () => {
    let harness: WorkerTestHarness;

    const clientConfig: TestClientConfig = {
        type: 's3',
        config: {},
        create(_config: any, _logger: Logger, _settings: any) {
            return { client: {} };
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
                { _op: 's3_sender', api_name: apiName },
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
