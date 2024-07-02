import 'jest-extended';
import type { S3Client } from '@aws-sdk/client-s3';
import { pMap } from '@terascope/utils';
import { makeClient, cleanupBucket } from './helpers.js';
import * as s3Helpers from '../../src/s3/s3-helpers.js';

describe('S3 Helpers - large objects', () => {
    const bucketName = 's3-test-helpers-bucket-large';
    let client: S3Client;

    beforeAll(async () => {
        client = await makeClient();
        await cleanupBucket(client, bucketName);
        await s3Helpers.createS3Bucket(client, { Bucket: bucketName });

        await pMap(
            new Array(10005).fill(0),
            async (el, i) => s3Helpers.putS3Object(client, { Bucket: bucketName, Key: `foo${i}` }),
            { concurrency: 100 }
        );
    });

    afterAll(async () => {
        await cleanupBucket(client, bucketName);
    });

    it('should delete all objects', async () => {
        await s3Helpers.deleteAllS3Objects(client, { Bucket: bucketName });
        const list = await s3Helpers.listS3Objects(client, { Bucket: bucketName });
        expect(list.Contents).toBeUndefined();
    });
});
