import 'jest-extended';
import { newTestJobConfig, WorkerTestHarness } from 'teraslice-test-harness';
import {
    AnyObject, APIConfig, ValidatedJobConfig,
    TestClientConfig, debugLogger, DataEncoding
} from '@terascope/job-components';
import { Format } from '@terascope/file-asset-apis';
import { DEFAULT_API_NAME } from '../../asset/src/s3_reader_api/interfaces.js';

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

    async function makeTest(config: AnyObject, apiConfig?: APIConfig) {
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
            const opConfig = { api_name: 's3_reader_api' };
            const apiConfig = { _name: 's3_reader_api', path: 'chillywilly' };

            await expect(makeTest(opConfig, apiConfig)).toResolve();
        });

        it('should not throw if _dead_letter_action are the same', async () => {
            const opConfig = {
                _op: 's3_reader',
                _dead_letter_action: 'throw',
                api_name: 's3_reader_api'
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
                api_name: 's3_reader_api'
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
                api_name: 's3_reader_api'
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
                api_name: 's3_reader_api'
            };

            const apiConfig = {
                _name: 's3_reader_api',
                path: '/chillywilly',
                _encoding: DataEncoding.JSON
            };

            await expect(makeTest(opConfig, apiConfig)).toReject();
        });

        it('will not throw if connection configs are specified in apis and not opConfig', async () => {
            const opConfig = { _op: 's3_reader', api_name: DEFAULT_API_NAME };
            const apiConfig = {
                _name: DEFAULT_API_NAME,
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
                (api: APIConfig) => api._name === DEFAULT_API_NAME
            );

            expect(validatedApiConfig).toMatchObject(apiConfig);
        });
    });
});
