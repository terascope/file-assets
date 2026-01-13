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
            const opConfig = {
                _op: 's3_reader',
                _api_name: DEFAULT_API_NAME
            };

            const apiConfig = {
                _name: DEFAULT_API_NAME,
            };
            await expect(makeTest(opConfig, apiConfig)).rejects.toThrow(/path.*This field is required and must be of type string/s);
        });

        it('should throw an error if no api is specified', async () => {
            const opConfig = {
                _op: 's3_reader',
                _api_name: DEFAULT_API_NAME
            };
            await expect(makeTest(opConfig)).toReject();
        });

        it('should not throw an error if valid config is given', async () => {
            const opConfig = {
                _op: 's3_reader',
                _api_name: DEFAULT_API_NAME
            };

            const apiConfig = {
                _name: DEFAULT_API_NAME,
                path: '/chillywilly',
            };

            await expect(makeTest(opConfig, apiConfig)).toResolve();
        });

        it('should ignore _dead_letter_action set in opConfig and use apiConfig _dead_letter_action', async () => {
            const opConfig = {
                _op: 's3_reader',
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
                _op: 's3_reader',
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
    });
});
