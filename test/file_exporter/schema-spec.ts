import 'jest-extended';
import { DataEncoding } from '@terascope/core-utils';
import {
    OpConfig, APIConfig, ValidatedJobConfig
} from '@terascope/job-components';
import { Format } from '@terascope/file-asset-apis';
import { newTestJobConfig, WorkerTestHarness } from 'teraslice-test-harness';
import { DEFAULT_API_NAME } from '../../asset/src/file_sender_api/interfaces.js';

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

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    describe('when validating the schema', () => {
        it('should throw an error if no path is specified', async () => {
            const opConfig = { _op: 'file_exporter', _api_name: DEFAULT_API_NAME };
            const apiConfig = {
                _name: DEFAULT_API_NAME,
                _dead_letter_action: 'throw'
            };
            await expect(makeTest(opConfig, apiConfig)).rejects.toThrow(/path.*This field is required and must be of type string/s);
        });

        it('should throw an error if no api is specified', async () => {
            const opConfig = { _op: 'file_exporter' };
            await expect(makeTest(opConfig)).rejects.toThrow(/_api_name.*This field is required and must be of type string/s);
        });

        it('should not throw an error if valid configs are given', async () => {
            const opConfig = {
                _op: 'file_exporter',
                _api_name: DEFAULT_API_NAME,
            };

            const apiConfig = {
                _name: DEFAULT_API_NAME,
                path: '/chillywilly',
                _dead_letter_action: 'throw'
            };

            await expect(makeTest(opConfig, apiConfig)).toResolve();
        });

        it('should ignore _dead_letter_action set in opConfig and use apiConfig _dead_letter_action', async () => {
            const opConfig = {
                _op: 'file_exporter',
                _dead_letter_action: 'none',
                _api_name: DEFAULT_API_NAME
            };

            const apiConfig = {
                _name: DEFAULT_API_NAME,
                path: '/chillywilly',
                _dead_letter_action: 'throw'
            };

            await makeTest(opConfig, apiConfig);

            const validatedApiConfig = harness.executionContext.config.apis.find(
                (api: APIConfig) => api._name === DEFAULT_API_NAME
            );

            expect(validatedApiConfig).toMatchObject(apiConfig);
        });

        it('should ignore _encoding set in opConfig and use apiConfig _encoding', async () => {
            const opConfig = {
                _op: 'file_exporter',
                _encoding: DataEncoding.RAW,
                _api_name: DEFAULT_API_NAME
            };

            const apiConfig = {
                _name: DEFAULT_API_NAME,
                path: '/chillywilly',
                _encoding: DataEncoding.JSON
            };

            await makeTest(opConfig, apiConfig);

            const validatedApiConfig = harness.executionContext.config.apis.find(
                (api: APIConfig) => api._name === DEFAULT_API_NAME
            );

            expect(validatedApiConfig).toMatchObject(apiConfig);
        });

        it('will not throw if connection configs are specified in apis and not opConfig', async () => {
            const opConfig = { _op: 'file_exporter', _api_name: DEFAULT_API_NAME };
            const apiConfig = {
                _name: DEFAULT_API_NAME,
                path: 'some/path',
                compression: 'none',
                size: 200,
                format: 'raw',
                line_delimiter: '\n'
            };

            const job = newTestJobConfig({
                apis: [apiConfig],
                operations: [
                    { _op: 'test-reader' },
                    opConfig
                ]
            });

            harness = new WorkerTestHarness(job);

            await harness.initialize();

            const validatedApiConfig = harness.executionContext.config.apis.find(
                (api: APIConfig) => api._name === DEFAULT_API_NAME
            );

            expect(validatedApiConfig).toMatchObject(apiConfig);
        });
    });
});
