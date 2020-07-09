import 'jest-extended';
import { OpConfig, APIConfig, ValidatedJobConfig } from '@terascope/job-components';
import { newTestJobConfig, WorkerTestHarness } from 'teraslice-test-harness';

describe('File exporter Schema', () => {
    let harness: WorkerTestHarness;

    async function makeTest(config: OpConfig, apiConfig?: APIConfig) {
        const testJob: Partial<ValidatedJobConfig> = {
            analytics: true,
            apis: [],
            operations: [
                { _op: 'test-reader' },
                config,

            ],
        };

        if (apiConfig) testJob!.apis!.push(apiConfig);

        const job = newTestJobConfig(testJob);

        harness = new WorkerTestHarness(job);

        await harness.initialize();
    }

    describe('when validating the schema', () => {
        it('should throw an error if no path is specified', async () => {
            const opConfig = { _op: 'file_exporter' };
            await expect(makeTest(opConfig)).toReject();
        });

        it('should not throw an error if path is specified in apiConfig', async () => {
            const opConfig = { _op: 'file_exporter' };
            const apiConfig = { _name: 'file_sender_api', path: '/chillywilly' };

            await expect(makeTest(opConfig, apiConfig)).toResolve();
        });

        it('should not throw an error if valid config is given', async () => {
            const opConfig = {
                _op: 'file_exporter',
                path: '/chillywilly'
            };

            await expect(makeTest(opConfig)).toResolve();
        });
    });
});
