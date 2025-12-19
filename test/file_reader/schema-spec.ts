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
            const opConfig = { _op: 'file_reader' };
            await expect(makeTest(opConfig)).toReject();
        });

        it('should not throw an error if no path is specified if apiConfig is set', async () => {
            const opConfig = { _op: 'file_reader', _api_name: 'file_reader_api' };
            const apiConfig = { _name: 'file_reader_api', path: 'some/path' };

            await expect(makeTest(opConfig, apiConfig)).toResolve();
        });

        it('should throw is path is specified and different than', async () => {
            const opConfig = { _op: 'file_reader', path: 'some/other', _api_name: 'file_reader_api' };
            const apiConfig = { _name: 'file_reader_api', path: 'some/path' };

            await expect(makeTest(opConfig, apiConfig)).toReject();
        });

        it('should throw is extra_args is specified and different from', async () => {
            const opConfig = { _op: 'file_reader', extra_args: { some: 'stuff' }, _api_name: 'file_reader_api' };
            const apiConfig = { _name: 'file_reader_api', path: 'some/path', extra_args: { some: 'other' } };

            await expect(makeTest(opConfig, apiConfig)).toReject();
        });

        it('should not throw if _dead_letter_action are the same', async () => {
            const opConfig = {
                _op: 'file_reader',
                _dead_letter_action: 'throw',
                _api_name: 'file_reader_api'
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
                _dead_letter_action: 'none',
                _api_name: 'file_reader_api'
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
                _encoding: DataEncoding.JSON,
                _api_name: 'file_reader_api'
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
                _encoding: DataEncoding.RAW,
                _api_name: 'file_reader_api'
            };

            const apiConfig = {
                _name: 'file_reader_api',
                path: '/chillywilly',
                _encoding: DataEncoding.JSON
            };

            await expect(makeTest(opConfig, apiConfig)).toReject();
        });

        it('will not throw if connection configs are specified in apis and not opConfig', async () => {
            const opConfig = { _op: 'file_reader', _api_name: 'file_reader_api' };
            const apiConfig = {
                _name: 'file_reader_api',
                path: 'some/path',
                compression: 'none',
                size: 200,
                format: 'raw',
                line_delimiter: '\n'
            };

            const job = newTestJobConfig({
                apis: [apiConfig],
                operations: [
                    opConfig,
                    {
                        _op: 'noop'
                    }
                ]
            });

            harness = new WorkerTestHarness(job);

            await harness.initialize();

            const validatedApiConfig = harness.executionContext.config.apis.find(
                (api: APIConfig) => api._name === 'file_reader_api'
            );

            expect(validatedApiConfig).toMatchObject(apiConfig);
        });
    });
});
