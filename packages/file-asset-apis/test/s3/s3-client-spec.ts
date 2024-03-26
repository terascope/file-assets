import 'jest-extended';
import type { CreateBucketOutput, S3Client } from '@aws-sdk/client-s3';
import { makeClient, cleanupBucket } from './helpers';
import * as s3Helpers from '../../src/s3/s3-helpers';
import { genS3ClientConfig } from '../../src/s3/createS3Client';

describe('S3 Helpers', () => {
    const bucketName = 's3-test-helpers-bucket';
    let bucket: CreateBucketOutput;
    let client: S3Client;

    describe('buckets', () => {
        const otherBucketName = 'other-helper-bucket';

        beforeAll(async () => {
            client = await makeClient();
            await cleanupBucket(client, bucketName);
            bucket = await s3Helpers.createS3Bucket(client, { Bucket: bucketName });
            await cleanupBucket(client, 'other-helper-bucket');
            await s3Helpers.createS3Bucket(client, { Bucket: 'other-helper-bucket' });
        });

        afterAll(async () => {
            await cleanupBucket(client, bucketName);
            await cleanupBucket(client, otherBucketName);
        });

        it('should create bucket', async () => {
            expect(bucket).toBeTruthy();
        });
    });

    describe('genS3ClientConfig', () => {
        it('should generate proper config', async () => {
            const c = {
                endpoint: 'http://192.168.65.1:49000',
                credentials: {
                    accessKeyId: 'minioadmin',
                    secretAccessKey: 'minioadmin'
                },
                forcePathStyle: true,
                sslEnabled: true,
                certLocation: '/tmp/terascope-ca-bundle.pem',
                region: 'us-east-1'
            };
            console.log(await genS3ClientConfig(c));
        });
    });
});
