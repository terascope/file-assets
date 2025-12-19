import 'jest-extended';
import { DataEncoding } from '@terascope/core-utils';
import {
    OpConfig, APIConfig, ValidatedJobConfig,
} from '@terascope/job-components';
import { Format } from '@terascope/file-asset-apis';
import { newTestJobConfig, WorkerTestHarness } from 'teraslice-test-harness';

describe('File Reader Schema', () => {
    let harness: WorkerTestHarness;

    async function makeTest(config: OpConfig, apiConfig?: APIConfig) {
        const testJob: Partial<ValidatedJobConfig> = {
            analytics: true,
            apis: [],
            operations: [
                { format: Format.ldjson, ...config },
                {
                    _op: 'noop',
                },
            ],
        };

        if (apiConfig) {
            testJob.apis!.push({
                format: Format.ldjson,
                ...apiConfig
            });
        }

        const job = newTestJobConfig(testJob);

        harness = new WorkerTestHarness(job);

        await harness.initialize();
    }

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    describe('when validating the schema', () => {
        it('should throw an error if no path is specified', async () => {
            const opConfig = {
                _op: 'file_reader',
                _api_name: 'file_reader_api'
            };

            const apiConfig = {
                _name: 'file_reader_api',
            };
            await expect(makeTest(opConfig, apiConfig)).rejects.toThrow(/path.*This field is required and must be of type string/s);
        });

        it('should throw an error if no api is specified', async () => {
            const opConfig = {
                _op: 'file_reader',
            };
            await expect(makeTest(opConfig)).toReject();
        });

        it('should ignore path set in opConfig and use apiConfig path', async () => {
            const opConfig = { _op: 'file_reader', path: 'some/other', _api_name: 'file_reader_api' };
            const apiConfig = { _name: 'file_reader_api', path: 'some/path' };

            await makeTest(opConfig, apiConfig);

            const validatedApiConfig = harness.executionContext.config.apis.find(
                (api: APIConfig) => api._name === 'file_reader_api'
            );

            expect(validatedApiConfig).toMatchObject(apiConfig);
        });

        it('should ignore extra_args set in opConfig and use apiConfig extra_args', async () => {
            const opConfig = { _op: 'file_reader', extra_args: { some: 'stuff' }, _api_name: 'file_reader_api' };
            const apiConfig = { _name: 'file_reader_api', path: 'some/path', extra_args: { some: 'other' } };

            await makeTest(opConfig, apiConfig);

            const validatedApiConfig = harness.executionContext.config.apis.find(
                (api: APIConfig) => api._name === 'file_reader_api'
            );

            expect(validatedApiConfig).toMatchObject(apiConfig);
        });

        it('should ignore _dead_letter_action set in opConfig and use apiConfig _dead_letter_action', async () => {
            const opConfig = {
                _op: 'file_reader',
                _dead_letter_action: 'none',
                _api_name: 'file_reader_api'
            };

            const apiConfig = {
                _name: 'file_reader_api',
                path: '/chillywilly',
                _dead_letter_action: 'throw'
            };

            await makeTest(opConfig, apiConfig);

            const validatedApiConfig = harness.executionContext.config.apis.find(
                (api: APIConfig) => api._name === 'file_reader_api'
            );

            expect(validatedApiConfig).toMatchObject(apiConfig);
        });

        it('should ignore _encoding set in opConfig and use apiConfig _encoding', async () => {
            const opConfig = {
                _op: 'file_reader',
                _encoding: DataEncoding.RAW,
                _api_name: 'file_reader_api'
            };

            const apiConfig = {
                _name: 'file_reader_api',
                path: '/chillywilly',
                _encoding: DataEncoding.JSON
            };

            await makeTest(opConfig, apiConfig);

            const validatedApiConfig = harness.executionContext.config.apis.find(
                (api: APIConfig) => api._name === 'file_reader_api'
            );

            expect(validatedApiConfig).toMatchObject(apiConfig);
        });
    });
});
