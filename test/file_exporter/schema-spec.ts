import 'jest-extended';
import {
    OpConfig, APIConfig, ValidatedJobConfig, DataEncoding
} from '@terascope/job-components';
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

        it('should not throw if _dead_letter_action are the same', async () => {
            const opConfig = {
                _op: 'file_exporter',
                _dead_letter_action: 'throw'
            };

            const apiConfig = {
                _name: 'file_sender_api',
                path: '/chillywilly',
                _dead_letter_action: 'throw'
            };

            await expect(makeTest(opConfig, apiConfig)).toResolve();
        });

        it('should throw if opConig _dead_letter_action is not a default value while apiConfig _dead_letter_action is set', async () => {
            const opConfig = {
                _op: 'file_exporter',
                _dead_letter_action: 'none'
            };

            const apiConfig = {
                _name: 'file_sender_api',
                path: '/chillywilly',
                _dead_letter_action: 'throw'
            };

            await expect(makeTest(opConfig, apiConfig)).toReject();
        });

        it('should not throw if _encoding are the same', async () => {
            const opConfig = {
                _op: 'file_exporter',
                _encoding: DataEncoding.JSON
            };

            const apiConfig = {
                _name: 'file_sender_api',
                path: '/chillywilly',
                _encoding: DataEncoding.JSON
            };

            await expect(makeTest(opConfig, apiConfig)).toResolve();
        });

        it('should throw if opConig _encoding is not a default value while apiConfig _encoding is set', async () => {
            const opConfig = {
                _op: 'file_exporter',
                _encoding: DataEncoding.RAW
            };

            const apiConfig = {
                _name: 'file_sender_api',
                path: '/chillywilly',
                _encoding: DataEncoding.JSON
            };

            await expect(makeTest(opConfig, apiConfig)).toReject();
        });
    });
});
