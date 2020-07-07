import 'jest-extended';
import { OpConfig, APIConfig, ValidatedJobConfig } from '@terascope/job-components';
import { newTestJobConfig, WorkerTestHarness } from 'teraslice-test-harness';

describe('S3 exporter Schema', () => {
    let harness: WorkerTestHarness;

    async function makeTest(config: OpConfig, apiConfig?: APIConfig) {
        const testJob: Partial<ValidatedJobConfig> = {
            analytics: true,
            apis: [],
            operations: [
                config,
                {
                    _op: 'noop',
                },
            ],
        };

        if (apiConfig) testJob!.apis!.push(apiConfig);

        const job = newTestJobConfig(testJob);

        harness = new WorkerTestHarness(job);

        await harness.initialize();
    }

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    describe('when validating the schema', () => {
        it('should throw an error if no path is specified', async () => {
            const opConfig = { _op: 'file_reader' };
            await expect(makeTest(opConfig)).toReject();
        });

        it('should not throw an error if no path is specified if apiConfig is set', async () => {
            const opConfig = { _op: 'file_reader' };
            const apiConfig = { _name: 'file_reader_api', path: 'some/path' };

            await expect(makeTest(opConfig, apiConfig)).toResolve();
        });

        it('should throw is path is specified and api is specified', async () => {
            const opConfig = { _op: 'file_reader', path: 'some/path' };
            const apiConfig = { _name: 'file_reader_api', path: 'some/path' };

            await expect(makeTest(opConfig, apiConfig)).toReject();
        });

        it('should throw is extra_args is specified and api is specified', async () => {
            const opConfig = { _op: 'file_reader', extra_args: { some: 'stuff' } };
            const apiConfig = { _name: 'file_reader_api', path: 'some/path' };

            await expect(makeTest(opConfig, apiConfig)).toReject();
        });
    });
});
