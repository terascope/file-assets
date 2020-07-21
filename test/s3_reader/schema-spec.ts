import 'jest-extended';
import { newTestJobConfig, WorkerTestHarness } from 'teraslice-test-harness';
import {
    AnyObject,
    APIConfig,
    ValidatedJobConfig,
    TestClientConfig,
    Logger,
    DataEncoding
} from '@terascope/job-components';

describe('S3 Reader Schema', () => {
    let harness: WorkerTestHarness;

    const clientConfig: TestClientConfig = {
        type: 's3',
        config: {},
        create(_config: any, _logger: Logger, _settings: any) {
            return { client: {} };
        },
        endpoint: 'default'
    };

    const clients = [clientConfig];

    async function makeTest(config: AnyObject, apiConfig?: APIConfig) {
        const opConfig = Object.assign(
            { _op: 's3_reader' },
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

        if (apiConfig) testJob!.apis!.push(apiConfig);

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
            const opConfig = {};
            const apiConfig = { _name: 's3_reader_api', path: 'chillywilly' };

            await expect(makeTest(opConfig, apiConfig)).toResolve();
        });

        it('should not throw if _dead_letter_action are the same', async () => {
            const opConfig = {
                _op: 's3_reader',
                _dead_letter_action: 'throw'
            };

            const apiConfig = {
                _name: 's3_reader_api',
                path: '/chillywilly',
                _dead_letter_action: 'throw'
            };

            await expect(makeTest(opConfig, apiConfig)).toResolve();
        });

        it('should throw if opConig _dead_letter_action is not a default value while apiConfig _dead_letter_action is set', async () => {
            const opConfig = {
                _op: 's3_reader',
                _dead_letter_action: 'none'
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
                _encoding: DataEncoding.JSON
            };

            const apiConfig = {
                _name: 's3_reader_api',
                path: '/chillywilly',
                _encoding: DataEncoding.JSON
            };

            await expect(makeTest(opConfig, apiConfig)).toResolve();
        });

        it('should throw if opConig _encoding is not a default value while apiConfig _encoding is set', async () => {
            const opConfig = {
                _op: 's3_reader',
                _encoding: DataEncoding.RAW
            };

            const apiConfig = {
                _name: 's3_reader_api',
                path: '/chillywilly',
                _encoding: DataEncoding.JSON
            };

            await expect(makeTest(opConfig, apiConfig)).toReject();
        });
    });
});
