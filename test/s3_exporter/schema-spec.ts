import 'jest-extended';
import { newTestJobConfig, WorkerTestHarness } from 'teraslice-test-harness';
import {
    Logger, DataEncoding, debugLogger
} from '@terascope/core-utils';
import {
    APIConfig, ValidatedJobConfig,
    TestClientConfig,
    OpConfig
} from '@terascope/job-components';
import { Format } from '@terascope/file-asset-apis';

describe('S3 exporter Schema', () => {
    const logger = debugLogger('s3 test');
    let harness: WorkerTestHarness;

    const clientConfig: TestClientConfig = {
        type: 's3',
        config: {},
        async createClient(_config: any, _logger: Logger, _settings: any) {
            return {
                client: {
                    send(_params: any) {}
                },
                logger
            };
        },
        endpoint: 'default'
    };

    const clients = [clientConfig];

    async function makeTest(config: Partial<OpConfig>, apiConfig?: APIConfig) {
        const opConfig = Object.assign(
            { _op: 's3_exporter', format: Format.ldjson },
            config
        );

        const testJob: Partial<ValidatedJobConfig> = {
            analytics: true,
            apis: [],
            operations: [
                { _op: 'test-reader' },
                opConfig,
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
            const opConfig = {
                _op: 's3_exporter',
                _api_name: 's3_sender_api'
            };

            const apiConfig = {
                _name: 's3_sender_api',
            };
            await expect(makeTest(opConfig, apiConfig)).rejects.toThrow(/path.*This field is required and must be of type string/s);
        });

        it('should throw an error if no api is specified', async () => {
            const opConfig = {
                _op: 's3_exporter',
                _api_name: 's3_sender_api'
            };
            await expect(makeTest(opConfig)).toReject();
        });

        it('should not throw an error if valid config is given', async () => {
            const opConfig = {
                _op: 's3_exporter',
                _api_name: 's3_sender_api'
            };

            const apiConfig = {
                _name: 's3_sender_api',
                path: '/chillywilly',
            };

            await expect(makeTest(opConfig, apiConfig)).toResolve();
        });

        it('should ignore _dead_letter_action set in opConfig and use apiConfig _dead_letter_action', async () => {
            const opConfig = {
                _op: 's3_exporter',
                _dead_letter_action: 'none',
                _api_name: 's3_sender_api'
            };

            const apiConfig = {
                _name: 's3_sender_api',
                path: '/chillywilly',
                _dead_letter_action: 'throw'
            };

            await makeTest(opConfig, apiConfig);

            const validatedApiConfig = harness.executionContext.config.apis.find(
                (api: APIConfig) => api._name === 's3_sender_api'
            );

            expect(validatedApiConfig).toMatchObject(apiConfig);
        });

        it('should ignore _encoding set in opConfig and use apiConfig _encoding', async () => {
            const opConfig = {
                _op: 's3_exporter',
                _encoding: DataEncoding.RAW,
                _api_name: 's3_sender_api'
            };

            const apiConfig = {
                _name: 's3_sender_api',
                path: '/chillywilly',
                _encoding: DataEncoding.JSON
            };

            await makeTest(opConfig, apiConfig);

            const validatedApiConfig = harness.executionContext.config.apis.find(
                (api: APIConfig) => api._name === 's3_sender_api'
            );

            expect(validatedApiConfig).toMatchObject(apiConfig);
        });
    });
});
