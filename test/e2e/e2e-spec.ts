import { jest } from '@jest/globals';
import 'jest-extended';
import fs from 'node:fs';
import { TerasliceClient } from 'teraslice-client-js';
import { DataEntity } from '@terascope/core-utils';
import { Format, S3Client, listS3Objects, getS3Object } from '@terascope/file-asset-apis';
import config from './config.js';
import { makeClient, upload, cleanupBucket, getBodyFromResults } from '../helpers/index.js';

const SOURCE_BUCKET = 'e2e-source';
const DEST_BUCKET = 'e2e-dest';
const TEST_PATH = '/test-data/';

const testData = [
    { id: 1, name: 'Alice', value: 100 },
    { id: 2, name: 'Bob', value: 200 },
    { id: 3, name: 'Charlie', value: 300 },
].map((obj) => DataEntity.make(obj));

describe('File Assets e2e', () => {
    jest.setTimeout(120 * 1000);

    let client: TerasliceClient;
    let s3Client: S3Client;

    beforeAll(async () => {
        client = new TerasliceClient({ host: config.TERASLICE_HOST });

        s3Client = await makeClient();

        await cleanupBucket(s3Client, SOURCE_BUCKET);
        await cleanupBucket(s3Client, DEST_BUCKET);
        await upload(s3Client, {
            format: Format.ldjson,
            bucket: SOURCE_BUCKET,
            path: `${SOURCE_BUCKET}${TEST_PATH}`,
        }, testData);
    });

    afterAll(async () => {
        await cleanupBucket(s3Client, SOURCE_BUCKET);
        await cleanupBucket(s3Client, DEST_BUCKET);
    });

    describe('asset upload', () => {
        it('should upload the asset bundle', async () => {
            const result = await client.assets.upload(
                fs.createReadStream(config.ASSET_ZIP_PATH)
            );

            expect(result.asset_id).toBeDefined();
        });

        it('should be discoverable on the cluster after upload', async () => {
            const records = await client.assets.getAsset('file');

            expect(records).not.toBeEmpty();
            expect(records[0].name).toBe('file');
        });
    });

    describe('s3 reader/exporter job', () => {
        it('should read from source bucket and write to destination bucket', async () => {
            const jobSpec = {
                name: 'e2e-s3-read-write',
                lifecycle: 'once',
                workers: 1,
                assets: ['file'],
                apis: [
                    {
                        _name: 's3_reader_api',
                        _connection: 'default',
                        path: `${SOURCE_BUCKET}${TEST_PATH}`,
                        format: Format.ldjson,
                        size: 100,
                    },
                    {
                        _name: 's3_sender_api',
                        _connection: 'default',
                        path: `${DEST_BUCKET}${TEST_PATH}`,
                        format: Format.ldjson,
                        file_per_slice: true,
                    }
                ],
                operations: [
                    { _op: 's3_reader', _api_name: 's3_reader_api' },
                    { _op: 's3_exporter', _api_name: 's3_sender_api' }
                ]
            };

            const job = await client.jobs.submit(jobSpec as any);
            const finalStatus = await job.waitForStatus(
                ['completed', 'failed'],
                1000,
                60000
            );

            expect(finalStatus).toBe('completed');
        });

        it('should have written the correct data to the destination bucket', async () => {
            const listResult = await listS3Objects(s3Client, { Bucket: DEST_BUCKET });

            expect(listResult.Contents).not.toBeEmpty();

            const allRecords: Record<string, any>[] = [];

            for (const obj of listResult.Contents ?? []) {
                const result = await getS3Object(s3Client, {
                    Bucket: DEST_BUCKET,
                    Key: obj.Key!,
                });
                const body = await getBodyFromResults(result);
                const lines = body.toString()
                    .trim()
                    .split('\n')
                    .filter(Boolean);
                for (const line of lines) {
                    allRecords.push(JSON.parse(line));
                }
            }

            expect(allRecords).toBeArrayOfSize(testData.length);

            for (const expected of testData) {
                const found = allRecords.find((r) => r.id === expected.id);
                expect(found).toBeDefined();
                expect(found?.name).toBe(expected.name);
                expect(found?.value).toBe(expected.value);
            }
        });
    });
});
