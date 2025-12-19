import 'jest-extended';
import { newTestJobConfig, WorkerTestHarness } from 'teraslice-test-harness';
import {
    debugLogger, DataEncoding
} from '@terascope/core-utils';
import {
    APIConfig, ValidatedJobConfig,
    TestClientConfig,
    OpConfig,
} from '@terascope/job-components';
import { Format } from '@terascope/file-asset-apis';

describe('S3 Reader Schema', () => {
    const logger = debugLogger('test');
    let harness: WorkerTestHarness;

    const clientConfig: TestClientConfig = {
        type: 's3',
        config: {},
        async createClient() {
            return {
                client: {},
                logger
            };
        },
        endpoint: 'default'
    };

    const clients = [clientConfig];

    async function makeTest(config: Partial<OpConfig>, apiConfig?: APIConfig) {
        const opConfig = Object.assign(
            { _op: 's3_reader', format: Format.ldjson },
            config
        );

        const testJob: Partial<ValidatedJobConfig> = {
            analytics: true,
            apis: [],
            operations: [
                opConfig,
                { _op: 'noop' },
            ],
        };

        if (apiConfig) {
            testJob!.apis!.push({
                format: Format.ldjson,
                ...apiConfig
            });
        }

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

        it('should not throw an error if valid config is given', async () => {
            const opConfig = {
                _op: 's3_reader',
                path: 'chillywilly',
            };

            await expect(makeTest(opConfig)).toResolve();
        });

        it('should not throw path is given in api', async () => {
            const opConfig = { _api_name: 's3_reader_api' };
            const apiConfig = { _name: 's3_reader_api', path: 'chillywilly' };

            await expect(makeTest(opConfig, apiConfig)).toResolve();
        });

        it('should not throw if _dead_letter_action are the same', async () => {
            const opConfig = {
                _op: 's3_reader',
                _dead_letter_action: 'throw',
                _api_name: 's3_reader_api'
            };

            const apiConfig = {
                _name: 's3_reader_api',
                path: '/chillywilly',
                _dead_letter_action: 'throw'
            };

            await expect(makeTest(opConfig, apiConfig)).toResolve();
        });

        it('should throw if opConfig _dead_letter_action is not a default value while apiConfig _dead_letter_action is set', async () => {
            const opConfig = {
                _op: 's3_reader',
                _dead_letter_action: 'none',
                _api_name: 's3_reader_api'
            };

            const apiConfig = {
                _name: 's3_reader_api',
                path: '/chillywilly',
                _dead_letter_action: 'throw'
            };

            await expect(makeTest(opConfig, apiConfig)).toReject();
        });

        it('should not throw if _encoding are the same', async () => {
            const opConfig = {
                _op: 's3_reader',
                _encoding: DataEncoding.JSON,
                _api_name: 's3_reader_api'
            };

            const apiConfig = {
                _name: 's3_reader_api',
                path: '/chillywilly',
                _encoding: DataEncoding.JSON
            };

            await expect(makeTest(opConfig, apiConfig)).toResolve();
        });

        it('should throw if opConfig _encoding is not a default value while apiConfig _encoding is set', async () => {
            const opConfig = {
                _op: 's3_reader',
                _encoding: DataEncoding.RAW,
                _api_name: 's3_reader_api'
            };

            const apiConfig = {
                _name: 's3_reader_api',
                path: '/chillywilly',
                _encoding: DataEncoding.JSON
            };

            await expect(makeTest(opConfig, apiConfig)).toReject();
        });

        it('will not throw if connection configs are specified in apis and not opConfig', async () => {
            const opConfig = { _op: 's3_reader', _api_name: 's3_reader_api' };
            const apiConfig = {
                _name: 's3_reader_api',
                path: '/chillywilly',
                format: Format.ldjson
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

            harness = new WorkerTestHarness(job, { clients });

            await harness.initialize();

            const validatedApiConfig = harness.executionContext.config.apis.find(
                (api: APIConfig) => api._name === 's3_reader_api'
            );

            expect(validatedApiConfig).toMatchObject(apiConfig);
        });
    });
});
