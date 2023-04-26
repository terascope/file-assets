import 'jest-extended';
import { DataEntity, AnyObject, toNumber } from '@terascope/job-components';
import { JobTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { Format, S3Client } from '@terascope/file-asset-apis';
import { makeClient, cleanupBucket, upload } from '../helpers';

describe('S3Reader job', () => {
    let harness: JobTestHarness;

    let client: S3Client;
    let clients: any;

    beforeAll(async () => {
        client = await makeClient();
        clients = [
            {
                type: 's3',
                endpoint: 'my-s3-connector',
                create: () => ({
                    client
                }),
            },
        ];
    });

    const topicData = [
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

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    describe('can read all data', () => {
        const bucket = 'job-test-ldjson';
        const dirPath = '/my/test/';
        const path = `${bucket}${dirPath}`;
        const format = Format.ldjson;

        beforeAll(async () => {
            await cleanupBucket(client, bucket);
            await upload(client, { format, bucket, path }, topicData);
        });

        afterAll(async () => {
            await cleanupBucket(client, bucket);
        });

        it('can run reader and slicer in long form job specification', async () => {
            const apiConfig = {
                _name: 's3_reader_api',
                connection: 'my-s3-connector',
                size: 100000,
                field_delimiter: ',',
                line_delimiter: '\n',
                compression: 'none',
                file_per_slice: false,
                path,
                format
            };

            const job = newTestJobConfig({
                analytics: true,
                apis: [apiConfig],
                operations: [
                    { _op: 's3_reader', api_name: 's3_reader_api' } as any,
                    { _op: 'noop' }
                ]
            });

            harness = new JobTestHarness(job, {
                clients,
            });

            await harness.initialize();

            const results = await harness.runToCompletion();

            expect(results).toBeArrayOfSize(1);

            const { data } = results[0];

            topicData.forEach((record) => {
                const carData = data.find((obj) => obj.car === record.car) as AnyObject;
                expect(carData).toBeDefined();
                expect(carData.color).toEqual(record.color);
                expect(toNumber(carData.price)).toEqual(record.price);
            });
        });

        it('can run reader and slicer in short form job specification', async () => {
            const opConfig = {
                _op: 's3_reader',
                connection: 'my-s3-connector',
                size: 100000,
                field_delimiter: ',',
                line_delimiter: '\n',
                compression: 'none',
                file_per_slice: false,
                path,
                format
            };

            const job = newTestJobConfig({
                analytics: true,
                apis: [],
                operations: [
                    opConfig,
                    { _op: 'noop' }
                ]
            });

            harness = new JobTestHarness(job, {
                clients,
            });

            await harness.initialize();

            const results = await harness.runToCompletion();

            expect(results).toBeArrayOfSize(1);

            const { data } = results[0];

            topicData.forEach((record) => {
                const carData = data.find((obj) => obj.car === record.car) as AnyObject;
                expect(carData).toBeDefined();
                expect(carData.color).toEqual(record.color);
                expect(toNumber(carData.price)).toEqual(record.price);
            });
        });
    });
});
