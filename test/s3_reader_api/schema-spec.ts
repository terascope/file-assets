import 'jest-extended';
import { newTestJobConfig, WorkerTestHarness } from 'teraslice-test-harness';
import { ValidatedJobConfig, TestClientConfig, debugLogger } from '@terascope/job-components';
import { S3ReaderAPIConfig } from '../../asset/src/s3_reader_api/interfaces';

describe('S3 Reader API Schema', () => {
    const logger = debugLogger('test');
    let harness: WorkerTestHarness;

    const clientConfig: TestClientConfig = {
        type: 's3',
        config: {},
        async createClient() {
            return {
                client: {},
                logger
            }
        },
        endpoint: 'default'
    };

    const clients = [clientConfig];

    async function makeTest(apiConfig: Partial<S3ReaderAPIConfig> = {}) {
        const apiName = 's3_reader_api';

        const config = Object.assign(
            { _name: apiName },
            apiConfig
        );

        const testJob: Partial<ValidatedJobConfig> = {
            analytics: true,
            apis: [config],
            operations: [
                { _op: 's3_reader', api_name: apiName },
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
    });
});
