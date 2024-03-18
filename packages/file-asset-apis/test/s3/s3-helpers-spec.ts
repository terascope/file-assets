import 'jest-extended';
import { mockClient } from 'aws-sdk-client-mock';
import { Readable } from 'stream';
import type { CreateBucketOutput, S3Client } from '@aws-sdk/client-s3';
import { S3Client as MClient, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { makeClient, cleanupBucket } from './helpers';
import * as s3Helpers from '../../src/s3/s3-helpers';

describe('S3 Helpers', () => {
    const bucketName = 's3-test-helpers-bucket';
    const retryBucket = 'retry-helper-bucket';
    let bucket: CreateBucketOutput;
    let client: S3Client;

    beforeAll(async () => {
        client = await makeClient();
        await cleanupBucket(client, bucketName);
        await cleanupBucket(client, retryBucket);
        bucket = await s3Helpers.createS3Bucket(client, { Bucket: bucketName });
    });

    afterAll(async () => {
        await cleanupBucket(client, bucketName);
    });

    describe('buckets', () => {
        const otherBucketName = 'other-helper-bucket';

        beforeAll(async () => {
            client = await makeClient();
            await cleanupBucket(client, 'other-helper-bucket');
            await s3Helpers.createS3Bucket(client, { Bucket: 'other-helper-bucket' });
        });

        afterAll(async () => {
            await cleanupBucket(client, otherBucketName);
        });

        it('should create bucket', async () => {
            expect(bucket).toBeTruthy();
        });

        it('should head bucket and check bucket exists', async () => {
            const exists = await s3Helpers.doesBucketExist(client, { Bucket: otherBucketName });
            expect(exists).toBeTrue();

            const nonExistent = await s3Helpers.doesBucketExist(client, { Bucket: 'non-existent-bucket' });
            expect(nonExistent).toBeFalse();
        });

        it('should list buckets', async () => {
            const list = await s3Helpers.listS3Buckets(client);

            const bucketNames = list.Buckets!.map((b) => b.Name);

            // ensure otherBucketName exists
            expect(bucketNames.includes(otherBucketName)).toBeTrue();

            // ensure bucketName exists
            expect(bucketNames.includes(bucketName)).toBeTrue();
        });

        it('should delete bucket', async () => {
            await s3Helpers.deleteS3Bucket(client, { Bucket: otherBucketName });

            const list = await s3Helpers.listS3Buckets(client);

            const bucketNames = list.Buckets!.map((b) => b.Name);

            // ensure otherBucketName is deleted
            expect(bucketNames.includes(otherBucketName)).toBeFalse();

            // ensure bucketName still exists
            expect(bucketNames.includes(bucketName)).toBeTrue();
        });
    });

    describe('objects', () => {
        beforeAll(async () => {
            const foo: any = Buffer.from(JSON.stringify({ foo: 'foo' }));
            const bar: any = Buffer.from(JSON.stringify({ bar: 'bar' }));

            await Promise.all([
                s3Helpers.putS3Object(client, { Bucket: bucketName, Key: 'foo', Body: foo }),
                s3Helpers.putS3Object(client, { Bucket: bucketName, Key: 'bar', Body: bar }),
                s3Helpers.putS3Object(client, { Bucket: bucketName, Key: 'baz' }),
                s3Helpers.putS3Object(client, { Bucket: bucketName, Key: 'bad' }),
            ]);
        });

        it('should get & put objects', async () => {
            const fetched = await s3Helpers.getS3Object(client, { Bucket: bucketName, Key: 'foo' });

            let result = '';
            if (fetched.Body && fetched.Body instanceof Readable) {
                for await (const chunk of fetched.Body) {
                    result += chunk;
                }
            }

            expect(JSON.parse(result)).toMatchObject({ foo: 'foo' });
            expect(fetched).toBeTruthy();
        });

        it('should tag objects', async () => {
            const tagged = await s3Helpers.tagS3Object(client, {
                Bucket: bucketName,
                Key: 'bar',
                Tagging: { TagSet: [{ Key: 'some', Value: 'thing' }] }
            });
            expect(tagged).toBeTruthy();
        });

        it('should list objects', async () => {
            const list = await s3Helpers.listS3Objects(client, { Bucket: bucketName });
            expect(list.Contents?.length).toBe(4);
        });

        it('should list objects w/prefix', async () => {
            const list = await s3Helpers.listS3Objects(client, { Bucket: bucketName, Prefix: 'ba' });
            expect(list.Contents?.length).toBe(3);
        });

        it('should list objects w/ start after', async () => {
            const list = await s3Helpers.listS3Objects(client, { Bucket: bucketName, StartAfter: 'bar' });
            expect(list.Contents?.length).toBe(2);
        });

        // added s3-large-spec.ts to show how to use listS3Objects w/ continuation token

        it('should list delete an object', async () => {
            const deleted = await s3Helpers.s3RequestWithRetry({
                client,
                func: s3Helpers.deleteS3Object,
                params: { Bucket: bucketName, Key: 'bad' }
            });
            expect(deleted).toBeTruthy();
        });

        it('should list delete multiple objects', async () => {
            const deleted = await s3Helpers.deleteS3Objects(client, {
                Bucket: bucketName,
                Delete: {
                    Objects: [
                        { Key: 'bad' },
                        { Key: 'baz' }]
                }
            });
            expect(deleted).toBeTruthy();
        });
    });

    describe('retry function wrapper', () => {
        beforeAll(async () => {
            client = await makeClient();

            await cleanupBucket(client, retryBucket);

            bucket = await s3Helpers.createS3Bucket(client, { Bucket: retryBucket });

            const foo: any = Buffer.from(JSON.stringify({ foo: 'foo' }));
            const bar: any = Buffer.from(JSON.stringify({ bar: 'bar' }));

            await Promise.all([
                s3Helpers.s3RequestWithRetry({
                    client,
                    func: s3Helpers.putS3Object,
                    params: { Bucket: retryBucket, Key: 'some', Body: foo }
                }),
                s3Helpers.s3RequestWithRetry({
                    client,
                    func: s3Helpers.putS3Object,
                    params: { Bucket: retryBucket, Key: 'thing', Body: bar }
                })
            ]);
        });

        afterAll(async () => {
            await cleanupBucket(client, retryBucket);
        });

        it('should return results if no error', async () => {
            const list = await s3Helpers.s3RequestWithRetry({
                client,
                func: s3Helpers.listS3Objects,
                params: { Bucket: retryBucket }
            }) as any;

            expect(list.Contents?.length).toBe(2);
        });

        it('should retry if initial attempt has a dns server error', async () => {
            const s3Mock = mockClient(MClient);

            s3Mock.on(ListObjectsV2Command)
                .rejectsOnce({ message: 'getaddrinfo EAI_AGAIN some.domain.com', Code: '500' })
                .resolvesOnce({ Contents: [{ Key: 'foo', Size: 1000 }] });

            const list = await s3Helpers.s3RequestWithRetry({
                client,
                func: s3Helpers.listS3Objects,
                params: { Bucket: retryBucket }
            }) as any;

            expect(list.Contents?.length).toBe(1);
            s3Mock.restore();
        });

        it('should retry if initial attempt has an aws retryable error', async () => {
            const s3Mock = mockClient(MClient);

            s3Mock.on(ListObjectsV2Command)
                .rejectsOnce({
                    $metadata: {
                        httpStatusCode: 503,
                        requestId: '17AFCE370C8A6960',
                        extendedRequestId: undefined,
                        cfId: undefined,
                        attempts: 1,
                        totalRetryDelay: 0
                    },
                    Code: 'SLOW DOWN'
                })
                .resolvesOnce({ Contents: [{ Key: 'foo', Size: 1000 }] });

            const list = await s3Helpers.s3RequestWithRetry({
                client,
                func: s3Helpers.listS3Objects,
                params: { Bucket: retryBucket }
            }) as any;

            expect(list.Contents?.length).toBe(1);

            s3Mock.restore();
        });

        it('should throw an error if error code is permanent', async () => {
            const s3Mock = mockClient(MClient);

            s3Mock.on(ListObjectsV2Command)
                .rejectsOnce({
                    $metadata: {
                        httpStatusCode: 404,
                        requestId: '17AFCE370C8A6960',
                        extendedRequestId: undefined,
                        cfId: undefined,
                        attempts: 1,
                        totalRetryDelay: 0
                    },
                    Code: 'Bucket Does Not Exist'
                });

            await expect(s3Helpers.s3RequestWithRetry({
                client,
                func: s3Helpers.listS3Objects,
                params: { Bucket: retryBucket }

            })).rejects.toThrow();

            s3Mock.restore();
        });

        it('should throw an error after 3 retry attempts', async () => {
            const s3Mock = mockClient(MClient);

            s3Mock.on(ListObjectsV2Command)
                .rejects({ message: 'getaddrinfo EAI_AGAIN some.domain.com', Code: '500' });

            await expect(s3Helpers.s3RequestWithRetry({
                client,
                func: s3Helpers.listS3Objects,
                params: { Bucket: retryBucket }
            })).rejects.toThrow();

            s3Mock.restore();
        });
    });
});
