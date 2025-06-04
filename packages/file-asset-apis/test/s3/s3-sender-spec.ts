import 'jest-extended';
import {
    DataEntity, debugLogger, isString, times
} from '@terascope/utils';
import { makeClient, cleanupBucket, getBodyFromResults } from './helpers.js';
import {
    Compression, Format, ChunkedFileSenderConfig,
    S3Sender, getS3Object, Compressor,
    listS3Buckets, MIN_CHUNK_SIZE_BYTES
} from '../../src/index.js';

describe('S3 Sender API', () => {
    const logger = debugLogger('s3-sender');
    const id = 'some-id';
    const bucket = 's3-sender-api';
    const dirPath = '/testing/';
    const path = `${bucket}${dirPath}`;
    const ensureBucket = 'testing-ensure';
    const metaRoute1 = 'route-1';
    const metaRoute2 = 'route-2';
    let client: any;

    beforeAll(async () => {
        client = await makeClient();

        await Promise.all([
            cleanupBucket(client, bucket),
            cleanupBucket(client, ensureBucket)
        ]);
    });

    afterAll(async () => {
        if (client) {
            await Promise.all([
                cleanupBucket(client, bucket),
                cleanupBucket(client, ensureBucket)
            ]);
        }
    });

    async function getBucketListNames(): Promise<string[]> {
        const { Buckets } = await listS3Buckets(client);

        if (!Buckets) return [];
        return Buckets.map((bucketObj) => bucketObj.Name).filter(isString);
    }

    it('can make sure a bucket exists, if not it will create one', async () => {
        const format = Format.ldjson;
        const compression = Compression.none;
        const testDirPath = 'bucket_test';
        const testPath = `${ensureBucket}/${testDirPath}`;

        const config: ChunkedFileSenderConfig = {
            path: testPath,
            id,
            format,
            compression,
            file_per_slice: true
        };

        const sender = new S3Sender(client, config, logger);

        const beforeBucketList = await getBucketListNames();
        expect(beforeBucketList.includes(ensureBucket)).toBeFalse();

        await sender.ensureBucket();

        const afterBucketList = await getBucketListNames();
        expect(afterBucketList.includes(ensureBucket)).toBeTrue();
    });

    it('can send csv data to s3', async () => {
        const data = [DataEntity.make({
            field0: 0,
            field1: 1,
            field2: 2,
            field3: 3,
            field4: 4,
            field5: 5
        })];
        const expectedResults = '0,1,2,3,4,5\n';
        const format = Format.csv;
        const compression = Compression.none;
        const compressor = new Compressor(compression);

        const config: ChunkedFileSenderConfig = {
            path,
            id,
            format,
            compression,
            file_per_slice: true
        };
        const sender = new S3Sender(client, config, logger);

        await sender.ensureBucket();

        await sender.send(data);

        const key = `testing/${id}.0.${format}`;

        const dbData = await getS3Object(client, {
            Bucket: bucket,
            Key: key,
        });
        const response = await getBodyFromResults(dbData);
        const fetchedData = await compressor.decompress(
            response
        );
        expect(fetchedData).toEqual(expectedResults);
    });

    it('can send compressed ldjson data to s3 using dynamic routes', async () => {
        const data = [
            DataEntity.make({ some: 'data' }, { 'standard:route': metaRoute1 }),
            DataEntity.make({ other: 'data' }, { 'standard:route': metaRoute2 })
        ];

        const format = Format.ldjson;
        const compression = Compression.gzip;
        const compressor = new Compressor(compression);

        const config: ChunkedFileSenderConfig = {
            path,
            id,
            format,
            compression,
            file_per_slice: true,
            dynamic_routing: true
        };
        const sender = new S3Sender(client, config, logger);

        await sender.ensureBucket();

        await sender.send(data);

        const key1 = `testing/${metaRoute1}/${id}.0.${format}.gz`;
        const key2 = `testing/${metaRoute2}/${id}.0.${format}.gz`;

        const [route1Record, route2Record] = await Promise.all([
            getS3Object(client, {
                Bucket: bucket,
                Key: key1,
            }),
            getS3Object(client, {
                Bucket: bucket,
                Key: key2,
            })
        ]);

        const [response1, response2] = await Promise.all([
            getBodyFromResults(route1Record),
            getBodyFromResults(route2Record)
        ]);

        const fetchedDataRoute1 = await compressor.decompress(
            response1
        );

        const fetchedDataRoute2 = await compressor.decompress(
            response2
        );

        expect(JSON.parse(fetchedDataRoute1)).toMatchObject({ some: 'data' });
        expect(JSON.parse(fetchedDataRoute2)).toMatchObject({ other: 'data' });
    });

    it('can send large ldjson, not compressed data to s3 (multipart)', async () => {
        const data = times(60_000, (index) => ({
            count: 'foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo',
            id: index,
            obj: { foo: 'bar' }
        }));

        const format = Format.ldjson;
        const compression = Compression.none;
        const compressor = new Compressor(compression);

        const config: ChunkedFileSenderConfig = {
            path,
            id,
            format,
            compression,
            file_per_slice: true,
            concurrency: 1
        };
        const sender = new S3Sender(client, config, logger);

        await sender.ensureBucket();

        await sender.send(data);

        const key = `testing/${id}.0.${format}`;

        const dbData = await getS3Object(client, {
            Bucket: bucket,
            Key: key,
        });

        const response = await getBodyFromResults(dbData);

        const fetchedData = await compressor.decompress(
            response
        );

        const expectedResults = `${data.map((obj) => JSON.stringify(obj)).join('\n')}\n`;
        expect(fetchedData).toEqual(expectedResults);
        expect(expectedResults.length).toBeGreaterThan(MIN_CHUNK_SIZE_BYTES);
    });
});
