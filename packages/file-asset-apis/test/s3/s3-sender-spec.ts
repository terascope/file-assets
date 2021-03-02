import 'jest-extended';
import { debugLogger, isString } from '@terascope/utils';
import { DataEntity } from '@terascope/job-components';
import { makeClient, cleanupBucket, getBodyFromResults } from './helpers';
import {
    Compression,
    Format,
    ChunkedFileSenderConfig,
    S3Sender,
    getS3Object,
    Compressor,
    listS3Buckets
} from '../../src';

describe('S3 Sender API', () => {
    const logger = debugLogger('s3-sender');
    const id = 'some-id';
    const bucket = 's3-sender-api';
    const dirPath = '/testing/';
    const path = `${bucket}${dirPath}`;
    const ensureBucket = 'testing-ensure';
    const client = makeClient();
    const metaRoute1 = 'route-1';
    const metaRoute2 = 'route-2';

    beforeAll(async () => {
        await Promise.all([
            cleanupBucket(client, bucket),
            cleanupBucket(client, ensureBucket)
        ]);
    });

    afterAll(async () => {
        await Promise.all([
            cleanupBucket(client, bucket),
            cleanupBucket(client, ensureBucket)
        ]);
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

        const fetchedData = await compressor.decompress(
            getBodyFromResults(dbData)
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

        const fetchedDataRoute1 = await compressor.decompress(
            getBodyFromResults(route1Record)
        );

        const fetchedDataRoute2 = await compressor.decompress(
            getBodyFromResults(route2Record)
        );

        expect(JSON.parse(fetchedDataRoute1)).toMatchObject({ some: 'data' });
        expect(JSON.parse(fetchedDataRoute2)).toMatchObject({ other: 'data' });
    });
});
