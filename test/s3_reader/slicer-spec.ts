import 'jest-extended';
import {
    AnyObject, isNil, DataEntity,
    TestClientConfig, debugLogger
} from '@terascope/job-components';
import { newTestJobConfig, SlicerTestHarness } from 'teraslice-test-harness';
import { Format, S3Client } from '@terascope/file-asset-apis';
import { makeClient, cleanupBucket, upload } from '../helpers/index.js';

describe('S3 slicer', () => {
    const logger = debugLogger('test');
    let harness: SlicerTestHarness;

    let client: S3Client;
    let clients: TestClientConfig[];

    beforeAll(async () => {
        client = await makeClient();
        clients = [
            {
                type: 's3',
                endpoint: 'default',
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

    async function makeTest(config: AnyObject = {}) {
        if (isNil(config.path)) throw new Error('test config must have path');
        if (isNil(config.format)) throw new Error('test config must have format');

        const opConfig = Object.assign(
            {},
            {
                _op: 's3_reader',
                size: 70,
                path: config.path,
                format: config.format
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

        harness = new SlicerTestHarness(job, {
            clients,
        });

        await harness.initialize();

        return harness;
    }

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    describe('when slicing JSON objects', () => {
        const bucket = 'slicer-test-json';
        const dirPath = '/my/test/';
        const path = `${bucket}${dirPath}`;
        const format = Format.json;

        beforeAll(async () => {
            await cleanupBucket(client, bucket);
            await upload(client, { format, bucket, path }, data);
        });

        afterAll(async () => {
            await cleanupBucket(client, bucket);
        });

        it('should generate whole-object slices.', async () => {
            const opConfig = { format, path };
            const expectedSlice = {
                path: 'my/test/test-id.0.json',
                offset: 0,
                total: 138,
                length: 138
            };

            const test = await makeTest(opConfig);
            const firstBatch = await test.createSlices();
            const secondBatch = await test.createSlices();
            const slices = firstBatch.concat(secondBatch);

            expect(slices).toBeArrayOfSize(2);
            expect(slices[0]).toMatchObject(expectedSlice);
            expect(slices[1]).toBeNull();
        });
    });

    describe('when slicing ldjson objects', () => {
        const bucket = 'slicer-test-ldjson';
        const dirPath = '/my/test/';
        const path = `${bucket}${dirPath}`;
        const format = Format.ldjson;

        beforeAll(async () => {
            await cleanupBucket(client, bucket);
            await upload(client, { format, bucket, path }, data);
        });

        afterAll(async () => {
            await cleanupBucket(client, bucket);
        });

        it('should chop up the data', async () => {
            const opConfig = { format, path };

            const expectedSlice1 = {
                offset: 0, length: 70, path: 'my/test/test-id.0.ldjson', total: 136
            };
            const expectedSlice2 = {
                offset: 69, length: 67, path: 'my/test/test-id.0.ldjson', total: 136
            };

            const test = await makeTest(opConfig);
            const firstBatch = await test.createSlices();
            const secondBatch = await test.createSlices();
            const slices = firstBatch.concat(secondBatch);

            expect(slices).toBeArrayOfSize(3);
            expect(slices[0]).toMatchObject(expectedSlice1);
            expect(slices[1]).toMatchObject(expectedSlice2);
            expect(slices[2]).toBeNull();
        });
    });
});
