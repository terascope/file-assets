import 'jest-extended';
import {
    DataEntity, toString, AnyObject, isNil, toNumber
} from '@terascope/job-components';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { S3ReaderFactoryAPI } from '../../asset/src/s3_reader_api/interfaces';
import { Format, SlicedFileResults } from '../../asset/src/__lib/interfaces';
import {
    makeClient, cleanupBucket, upload, testWorkerId
} from '../helpers';

describe('S3 API Reader', () => {
    let harness: WorkerTestHarness;

    const client = makeClient();

    const clients = [
        {
            type: 's3',
            endpoint: 'my-s3-connector',
            create: () => ({
                client
            }),
        },
    ];

    const data = [
        {
            car: 'Audi',
            price: 40000,
            color: 'blue'
        }, {
            car: 'BMW',
            price: 35000,
            color: 'black'
        }, {
            car: 'Porsche',
            price: 60000,
            color: 'green'
        }
    ].map((obj) => DataEntity.make(obj));

    async function makeApiTest(config: AnyObject) {
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
                compression: 'none'
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

        return harness.getAPI<S3ReaderFactoryAPI>('s3_reader_api');
    }

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    describe('tsv data', () => {
        const bucket = 'api-reader-test-tsv';
        const dirPath = '/my/test/';
        const path = `${bucket}${dirPath}`;
        const slicePath = `${dirPath}${testWorkerId}`;
        const format = Format.tsv;

        beforeAll(async () => {
            await cleanupBucket(client, bucket);
            await upload(client, { format, bucket, path }, data);
        });

        afterAll(async () => {
            await cleanupBucket(client, bucket);
        });

        const slice: SlicedFileResults = {
            length: 10000,
            offset: 0,
            path: slicePath,
            total: 10000
        };

        it('can be fetched', async () => {
            const opConfig = { path, format };

            const apiManager = await makeApiTest(opConfig);
            const api = await apiManager.create('tsv', {});
            const results = await api.read(slice);

            expect(results).toBeArrayOfSize(4);

            const cars = results.map((obj) => obj.field1);
            const price = results.map((obj) => obj.field2);
            const color = results.map((obj) => obj.field3);

            data.forEach((record) => {
                expect(cars).toContain(record.car);
                expect(price).toContain(toString(record.price));
                expect(color).toContain(record.color);
            });
        });
    });

    describe('csv data', () => {
        const bucket = 'api-reader-test-csv';
        const dirPath = '/my/test/';
        const path = `${bucket}${dirPath}`;
        const slicePath = `${dirPath}${testWorkerId}`;
        const format = Format.csv;

        beforeAll(async () => {
            await cleanupBucket(client, bucket);
            await upload(client, { format, bucket, path }, data);
        });

        afterAll(async () => {
            await cleanupBucket(client, bucket);
        });

        const slice: SlicedFileResults = {
            length: 10000,
            offset: 0,
            path: slicePath,
            total: 10000
        };

        it('can be fetched', async () => {
            const opConfig = { path, format };

            const apiManager = await makeApiTest(opConfig);
            const api = await apiManager.create('csv', {});
            const results = await api.read(slice);

            expect(results).toBeArrayOfSize(4);

            const cars = results.map((obj) => obj.field1);
            const price = results.map((obj) => obj.field2);
            const color = results.map((obj) => obj.field3);

            data.forEach((record) => {
                expect(cars).toContain(record.car);
                expect(price).toContain(toString(record.price));
                expect(color).toContain(record.color);
            });
        });
    });

    describe('json data', () => {
        const bucket = 'api-reader-test-json';
        const dirPath = '/my/test/';
        const path = `${bucket}${dirPath}`;
        const slicePath = `${dirPath}${testWorkerId}`;
        const format = Format.json;

        beforeAll(async () => {
            await cleanupBucket(client, bucket);
            await upload(client, { format, bucket, path }, data);
        });

        afterAll(async () => {
            await cleanupBucket(client, bucket);
        });

        const slice: SlicedFileResults = {
            length: 10000,
            offset: 0,
            path: slicePath,
            total: 10000
        };

        it('can be fetched', async () => {
            const opConfig = { path, format };

            const apiManager = await makeApiTest(opConfig);
            const api = await apiManager.create('json', {});
            const results = await api.read(slice);

            expect(results).toBeArrayOfSize(3);

            data.forEach((record) => {
                const carData = results.find((obj) => obj.car === record.car) as AnyObject;
                expect(carData).toBeDefined();
                expect(carData.color).toEqual(record.color);
                expect(toNumber(carData.price)).toEqual(record.price);
            });
        });
    });

    describe('ldjson data', () => {
        const bucket = 'api-reader-test-ldjson';
        const dirPath = '/my/test/';
        const path = `${bucket}${dirPath}`;
        const slicePath = `${dirPath}${testWorkerId}`;
        const format = Format.ldjson;

        beforeAll(async () => {
            await cleanupBucket(client, bucket);
            await upload(client, { format, bucket, path }, data);
        });

        afterAll(async () => {
            await cleanupBucket(client, bucket);
        });

        const slice: SlicedFileResults = {
            length: 10000,
            offset: 0,
            path: slicePath,
            total: 10000
        };

        it('can be fetched', async () => {
            const opConfig = { path, format };

            const apiManager = await makeApiTest(opConfig);
            const api = await apiManager.create('ldjson', {});
            const results = await api.read(slice);

            expect(results).toBeArrayOfSize(3);

            data.forEach((record) => {
                const carData = results.find((obj) => obj.car === record.car) as AnyObject;
                expect(carData).toBeDefined();
                expect(carData.color).toEqual(record.color);
                expect(toNumber(carData.price)).toEqual(record.price);
            });
        });
    });

    describe('raw data', () => {
        const bucket = 'api-reader-test-raw';
        const dirPath = '/my/test/';
        const path = `${bucket}${dirPath}`;
        const slicePath = `${dirPath}${testWorkerId}`;
        const format = Format.raw;
        const rawData = ['chillywilly', 'johndoe'];
        const newData = rawData.map((name) => DataEntity.make({ data: name }));

        beforeAll(async () => {
            await cleanupBucket(client, bucket);
            await upload(client, { format, bucket, path }, newData);
        });

        afterAll(async () => {
            await cleanupBucket(client, bucket);
        });

        const slice: SlicedFileResults = {
            length: 10000,
            offset: 0,
            path: slicePath,
            total: 10000
        };

        it('can be fetched', async () => {
            const opConfig = { path, format };

            const apiManager = await makeApiTest(opConfig);
            const api = await apiManager.create('raw', {});
            const results = await api.read(slice);

            expect(results).toBeArrayOfSize(2);

            results.forEach((record) => {
                expect(rawData).toContain(record.data);
            });
        });
    });
});
