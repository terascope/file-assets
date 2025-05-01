import 'jest-extended';
import { debugLogger, times } from '@terascope/utils';
import { makeClient, cleanupBucket, getBodyFromResults } from './helpers.js';
import {
    Compression, Format, ChunkedFileSenderConfig,
    S3Sender, getS3Object, Compressor,
    MIN_CHUNK_SIZE_BYTES
} from '../../src/index.js';

describe('S3 Sender API', () => {
    const logger = debugLogger('s3-sender');
    const id = 'some-id';
    const bucket = 's3-sender-api';
    const dirPath = '/testing/';
    const path = `${bucket}${dirPath}`;
    const ensureBucket = 'testing-ensure';
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

    describe('sending large ldjson, not compressed data to s3 (multipart)', () => {
        it('should work normally', async () => {
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

            await sender.simpleSend(data);

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

        it('should work with buffer', async () => {
            const data = times(60_000, (index) => ({
                count: 'foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo',
                id: index,
                obj: { foo: 'bar' }
            }));

            const format = Format.ldjson;
            const compression = Compression.none;
            const compressor = new Compressor(compression);

            const config: ChunkedFileSenderConfig = {
                path: 's3-sender-api/testbuff',
                id,
                format,
                compression,
                file_per_slice: true,
                concurrency: 1
            };
            const sender = new S3Sender(client, config, logger);

            await sender.ensureBucket();

            await sender.simpleSend(data, 'buffer');

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

        it('should work with batched buffer', async () => {
            const data = times(60_000, (index) => ({
                count: 'foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo',
                id: index,
                obj: { foo: 'bar' }
            }));

            const format = Format.ldjson;
            const compression = Compression.none;
            const compressor = new Compressor(compression);

            const config: ChunkedFileSenderConfig = {
                path: 's3-sender-api/testbatch',
                id,
                format,
                compression,
                file_per_slice: true,
                concurrency: 1
            };
            const sender = new S3Sender(client, config, logger);

            await sender.ensureBucket();

            await sender.simpleSend(data, 'batchBuffer');

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
});
