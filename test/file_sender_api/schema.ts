import 'jest-extended';
import { newTestJobConfig, WorkerTestHarness } from 'teraslice-test-harness';
import { ValidatedJobConfig } from '@terascope/job-components';
import { ReaderFileAPI } from '../../asset/src/file_sender_api/interfaces';

describe('File Sender API Schema', () => {
    let harness: WorkerTestHarness;

    async function makeTest(apiConfig: Partial<ReaderFileAPI> = {}) {
        const apiName = 'file_sender_api';

        const config = Object.assign(
            { _name: apiName },
            apiConfig
        );

        const testJob: Partial<ValidatedJobConfig> = {
            analytics: true,
            apis: [config],
            operations: [
                { _op: 'file_exporter', api_name: apiName },
                { _op: 'noop' },
            ],
        };

        const job = newTestJobConfig(testJob);

        harness = new WorkerTestHarness(job);
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
