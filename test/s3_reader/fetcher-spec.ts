import 'jest-extended';
import {
    DataEntity, isNil,
    toNumber, debugLogger
} from '@terascope/core-utils';
import { OpConfig, TestClientConfig } from '@terascope/job-components';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { Format, FileSlice, S3Client } from '@terascope/file-asset-apis';
import {
    makeClient, cleanupBucket, upload,
    testWorkerId
} from '../helpers/index.js';
import { S3ReaderAPIConfig } from '../../asset/src/s3_reader_api/interfaces.js';

describe('S3Reader fetcher', () => {
    const logger = debugLogger('test');
    let harness: WorkerTestHarness;
    let client: S3Client;
    let clients: TestClientConfig[];

    beforeAll(async () => {
        client = await makeClient();
        clients = [
            {
                type: 's3',
                endpoint: 'my-s3-connector',
                async createClient() {
                    return {
                        client,
                        logger
                    };
                },
            },
        ];
    });

    const data = [
        {
            car: 'Audi',
            price: 40000,
            color: 'blue'
        },
        {
            car: 'BMW',
            price: 35000,
            color: 'black'
        },
        {
            car: 'Porsche',
            price: 60000,
            color: 'green'
        }
    ].map((obj) => DataEntity.make(obj));

    async function makeTest(config: { _op: Partial<OpConfig>; api: Partial<S3ReaderAPIConfig> }) {
        if (isNil(config.api.path)) throw new Error('test config must have path');
        if (isNil(config.api.format)) throw new Error('test config must have format');

        const opConfig = Object.assign(
            {},
            {
                _op: 's3_reader',
                _api_name: 's3_reader_api'
            },
            config._op
        );

        const apiConfig = Object.assign(
            {},
            {
                _name: 's3_reader_api',
                _connection: 'my-s3-connector',
                size: 100000,
                field_delimiter: ',',
                line_delimiter: '\n',
                compression: 'none',
                format: Format.ldjson
            },
            config.api
        );

        const job = newTestJobConfig({
            analytics: true,
            apis: [apiConfig],
            operations: [
                opConfig,
                {
                    _op: 'noop'
                }
            ]
        });

        harness = new WorkerTestHarness(job, {
            clients,
        });

        await harness.initialize();

        return harness;
    }

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    describe('can read data', () => {
        const bucket = 'fetcher-test-ldjson';
        const dirPath = '/my/test/';
        const path = `${bucket}${dirPath}`;
        const slicePath = `${dirPath}${testWorkerId}.0.ldjson`;
        const format = Format.ldjson;

        beforeAll(async () => {
            await cleanupBucket(client, bucket);
            await upload(client, { format, bucket, path }, data);
        });

        afterAll(async () => {
            await cleanupBucket(client, bucket);
        });

        const slice: FileSlice = {
            length: 10000,
            offset: 0,
            path: slicePath,
            total: 10000
        };

        it('can be fetched', async () => {
            const apiConfig = { path, format };

            const test = await makeTest({ _op: {}, api: apiConfig });

            const result = await test.runSlice(slice);

            expect(result).toBeArrayOfSize(3);

            data.forEach((record) => {
                const carData = result.find((obj) => obj.car === record.car) as Record<string, any>;
                expect(carData).toBeDefined();
                expect(carData.color).toEqual(record.color);
                expect(toNumber(carData.price)).toEqual(record.price);
            });
        });
    });
});
