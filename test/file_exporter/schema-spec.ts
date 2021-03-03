import 'jest-extended';
import {
    OpConfig, APIConfig, ValidatedJobConfig, DataEncoding
} from '@terascope/job-components';
import { Format } from '@terascope/file-asset-apis';
import { newTestJobConfig, WorkerTestHarness } from 'teraslice-test-harness';

describe('File exporter Schema', () => {
    let harness: WorkerTestHarness;

    async function makeTest(config: OpConfig, apiConfig?: APIConfig) {
        const testJob: Partial<ValidatedJobConfig> = {
            analytics: true,
            apis: [],
            operations: [
                { _op: 'test-reader' },
                { format: Format.ldjson, ...config },
            ],
        };

        if (apiConfig) {
            testJob!.apis!.push({
                format: Format.ldjson,
                ...apiConfig
            });
        }

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
            const opConfig = { _op: 'file_exporter', api_name: 'file_sender_api' };
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
                _dead_letter_action: 'throw',
                api_name: 'file_sender_api'
            };

            const apiConfig = {
                _name: 'file_sender_api',
                path: '/chillywilly',
                _dead_letter_action: 'throw'
            };

            await expect(makeTest(opConfig, apiConfig)).toResolve();
        });

        it('should throw if opConfig _dead_letter_action is not a default value while apiConfig _dead_letter_action is set', async () => {
            const opConfig = {
                _op: 'file_exporter',
                _dead_letter_action: 'none',
                api_name: 'file_sender_api'
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
                _encoding: DataEncoding.JSON,
                api_name: 'file_sender_api'
            };

            const apiConfig = {
                _name: 'file_sender_api',
                path: '/chillywilly',
                _encoding: DataEncoding.JSON
            };

            await expect(makeTest(opConfig, apiConfig)).toResolve();
        });

        it('should throw if opConfig _encoding is not a default value while apiConfig _encoding is set', async () => {
            const opConfig = {
                _op: 'file_exporter',
                _encoding: DataEncoding.RAW,
                api_name: 'file_sender_api'
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
