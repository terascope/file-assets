import 'jest-extended';
import {
    OpConfig, APIConfig, ValidatedJobConfig, DataEncoding
} from '@terascope/job-components';
import { newTestJobConfig, WorkerTestHarness } from 'teraslice-test-harness';

describe('File Reader Schema', () => {
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

        it('should not throw if _dead_letter_action are the same', async () => {
            const opConfig = {
                _op: 'file_reader',
                _dead_letter_action: 'throw'
            };

            const apiConfig = {
                _name: 'file_reader_api',
                path: '/chillywilly',
                _dead_letter_action: 'throw'
            };

            await expect(makeTest(opConfig, apiConfig)).toResolve();
        });

        it('should throw if opConfig _dead_letter_action is not a default value while apiConfig _dead_letter_action is set', async () => {
            const opConfig = {
                _op: 'file_reader',
                _dead_letter_action: 'none'
            };

            const apiConfig = {
                _name: 'file_reader_api',
                path: '/chillywilly',
                _dead_letter_action: 'throw'
            };

            await expect(makeTest(opConfig, apiConfig)).toReject();
        });

        it('should not throw if _encoding are the same', async () => {
            const opConfig = {
                _op: 'file_reader',
                _encoding: DataEncoding.JSON
            };

            const apiConfig = {
                _name: 'file_reader_api',
                path: '/chillywilly',
                _encoding: DataEncoding.JSON
            };

            await expect(makeTest(opConfig, apiConfig)).toResolve();
        });

        it('should throw if opConfig _encoding is not a default value while apiConfig _encoding is set', async () => {
            const opConfig = {
                _op: 'file_reader',
                _encoding: DataEncoding.RAW
            };

            const apiConfig = {
                _name: 'file_reader_api',
                path: '/chillywilly',
                _encoding: DataEncoding.JSON
            };

            await expect(makeTest(opConfig, apiConfig)).toReject();
        });
    });
});
