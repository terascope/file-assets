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

    async function makeTest(config: Partial<OpConfig>) {
        if (isNil(config.path)) throw new Error('test config must have path');
        if (isNil(config.format)) throw new Error('test config must have format');

        const opConfig = Object.assign(
            {},
            {
                _op: 's3_reader',
                connection: 'my-s3-connector',
                size: 100000,
                field_delimiter: ',',
                line_delimiter: '\n',
                compression: 'none',
                format: Format.ldjson
            },
            config
        );

        const job = newTestJobConfig({
            analytics: true,
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

    describe('can reade data', () => {
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
            const opConfig = { path, format };

            const test = await makeTest(opConfig);
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
