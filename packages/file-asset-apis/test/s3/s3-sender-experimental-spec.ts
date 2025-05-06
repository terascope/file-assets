import 'jest-extended';
import { DataFrame } from '@terascope/data-mate';
import {
    DataEntity, debugLogger, pMap, times
} from '@terascope/utils';
import { DataTypeConfig } from '@terascope/data-types';
import { makeClient, cleanupBucket, getBodyFromResults } from './helpers.js';
import {
    Compression, Format, ChunkedFileSenderConfig,
    S3Sender, getS3Object, Compressor,
    MIN_CHUNK_SIZE_BYTES,
    S3Slicer,
    FileSlice,
    S3Fetcher
} from '../../src/index.js';

describe('S3 Sender API', () => {
    const logger = debugLogger('s3-sender');
    const id = 'some-id';
    const bucket = 's3-sender-api';
    const ensureBucket = 'testing-ensure';
    let client: any;

    const format = Format.ldjson;
    const compression = Compression.none;
    const compressor = new Compressor(compression);

    const getExpectedSlices = (path = 'testing') => ([
        {
            length: 10485760,
            offset: 0,
            path: `${path}/some-id.0.ldjson`,
            total: 40146054
        },
        {
            length: 10485761,
            offset: 10485759,
            path: `${path}/some-id.0.ldjson`,
            total: 40146054
        },
        {
            length: 10485761,
            offset: 20971519,
            path: `${path}/some-id.0.ldjson`,
            total: 40146054
        },
        {
            offset: 31457279,
            length: 8688775,
            path: `${path}/some-id.0.ldjson`,
            total: 40146054
        }
    ]);

    const data = times(200_000, (index) => {
        let count = index % 0 === 0
            ? 'foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo,foo'
            : 'bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar,bar';
        if (index % 7 === 0) {
            count = 'abcdefghijklmnopqrstuvwxyz,abcdefghijklmnopqrstuvwxyz,abcdefghijklmnopqrstuvwxyz,abcdefghijklmnopqrstuvwxyz,abcdefghijklmnopqrstuvwxyz,abcdefghijklmnopqrstuvwxyz,abcdefghijklmnopqrstuvwxyz';
        }
        return ({
            count,
            id: index,
            obj: { foo: 'bar' }
        });
    });

    const expectedResults = `${data.map((obj) => JSON.stringify(obj)).join('\n')}\n`;

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
            const dirPath = 'testing';
            const path = `${bucket}/${dirPath}/`;
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

            const slicer = new S3Slicer(client, {
                file_per_slice: false,
                format: Format.ldjson,
                size: 10 * 1024 * 1024,
                path,
            }, logger);

            let slices: FileSlice[] = [];
            let isDone = false;
            while (!isDone) {
                const slicesOrDone = await slicer.slice();
                if (slicesOrDone == null) {
                    isDone = true;
                } else {
                    slices = slices.concat(slicesOrDone);
                }
            }
            expect(slices).toEqual(getExpectedSlices(dirPath));

            const fetcher = new S3Fetcher(client, {
                format: Format.ldjson,
                path,
                file_per_slice: false,
                compression: Compression.none,
            }, logger);

            let dataRead: DataEntity[] = [];
            await pMap(slices, async (slice, i) => {
                const record = await fetcher.read(slice);
                dataRead = dataRead.concat(record);
                return { total: record.length };
            }, { concurrency: 1 });

            expect(dataRead).toEqual(data);

            // [ 49749, 49695, 49464, 49461, 1631 ]
            // [ 49749, 49695, 49464, 49461, 1631 ]
            // [ 49749, 49695, 49464, 49461, 1631 ]

            const dbData = await getS3Object(client, {
                Bucket: bucket,
                Key: key,
            });

            const response = await getBodyFromResults(dbData);

            const fetchedData = await compressor.decompress(
                response
            );

            expect(fetchedData).toEqual(expectedResults);
            expect(expectedResults.length).toBeGreaterThan(MIN_CHUNK_SIZE_BYTES);
        });

        it('should work with buffer', async () => {
            const dirPath = 'testbuff';
            const path = `${bucket}/${dirPath}/`;
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

            await sender.simpleSend(data, 'buffer');

            // const key = `testing/${id}.0.${format}`;

            const slicer = new S3Slicer(client, {
                file_per_slice: false,
                format: Format.ldjson,
                size: 10 * 1024 * 1024,
                path,
            }, logger);

            let slices: FileSlice[] = [];
            let isDone = false;
            while (!isDone) {
                const slicesOrDone = await slicer.slice();
                if (slicesOrDone == null) {
                    isDone = true;
                } else {
                    slices = slices.concat(slicesOrDone);
                }
            }
            expect(slices).toEqual(getExpectedSlices(dirPath));

            const fetcher = new S3Fetcher(client, {
                format: Format.ldjson,
                path,
                file_per_slice: false,
                compression: Compression.none,
            }, logger);

            let dataRead: DataEntity[] = [];
            await pMap(slices, async (slice, i) => {
                const record = await fetcher.read(slice);
                dataRead = dataRead.concat(record);
                return { total: record.length };
            }, { concurrency: 1 });
            expect(dataRead).toEqual(data);

            // const dbData = await getS3Object(client, {
            //     Bucket: bucket,
            //     Key: key,
            // });

            // const response = await getBodyFromResults(dbData);

            // const fetchedData = await compressor.decompress(
            //     response
            // );

            // expect(fetchedData).toEqual(expectedResults);
            // expect(expectedResults.length).toBeGreaterThan(MIN_CHUNK_SIZE_BYTES);
        });

        it('should work with batched buffer', async () => {
            const dirPath = 'testbatch';
            const path = `${bucket}/${dirPath}/`;
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

            await sender.simpleSend(data, 'batchBuffer');

            // const key = `testing/${id}.0.${format}`;

            const slicer = new S3Slicer(client, {
                file_per_slice: false,
                format: Format.ldjson,
                size: 10 * 1024 * 1024,
                path,
            }, logger);

            let slices: FileSlice[] = [];
            let isDone = false;
            while (!isDone) {
                const slicesOrDone = await slicer.slice();
                if (slicesOrDone == null) {
                    isDone = true;
                } else {
                    slices = slices.concat(slicesOrDone);
                }
            }
            expect(slices).toEqual(getExpectedSlices(dirPath));

            const fetcher = new S3Fetcher(client, {
                format: Format.ldjson,
                path,
                file_per_slice: false,
                compression: Compression.none,
            }, logger);

            let dataRead: DataEntity[] = [];
            await pMap(slices, async (slice) => {
                const record = await fetcher.read(slice);
                dataRead = dataRead.concat(record);
                return { total: record.length };
            }, { concurrency: 1 });
            expect(dataRead).toEqual(data);

            // const dbData = await getS3Object(client, {
            //     Bucket: bucket,
            //     Key: key,
            // });

            // const response = await getBodyFromResults(dbData);

            // const fetchedData = await compressor.decompress(
            //     response
            // );

            // expect(fetchedData).toEqual(expectedResults);
            // expect(expectedResults.length).toBeGreaterThan(MIN_CHUNK_SIZE_BYTES);
        });

        it('should work with data frame batch', async () => {
            const dirPath = 'testarydf';
            const path = `${bucket}/${dirPath}/`;
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
            const tc: DataTypeConfig = {
                fields: {
                    count: { type: 'Text' },
                    id: { type: 'Number' },
                    obj: { type: 'Object' }
                }
            };

            const frame = DataFrame.fromJSON(tc, data);

            await sender.simpleSend(data, 'batchBuffer', frame);

            const slicer = new S3Slicer(client, {
                file_per_slice: false,
                format: Format.ldjson,
                size: 10 * 1024 * 1024,
                path,
            }, logger);

            let slices: FileSlice[] = [];
            let isDone = false;
            while (!isDone) {
                const slicesOrDone = await slicer.slice();
                if (slicesOrDone == null) {
                    isDone = true;
                } else {
                    slices = slices.concat(slicesOrDone);
                }
            }
            expect(slices).toEqual(getExpectedSlices(dirPath));

            const fetcher = new S3Fetcher(client, {
                format: Format.ldjson,
                path,
                file_per_slice: false,
                compression: Compression.none,
            }, logger);

            let dataRead: DataEntity[] = [];
            await pMap(slices, async (slice) => {
                const record = await fetcher.read(slice);
                dataRead = dataRead.concat(record);
                return { total: record.length };
            }, { concurrency: 1 });

            expect(dataRead).toEqual(data);

            // const key = `testarybatch/${id}.0.${format}`;
            // const { Contents } = await listS3Objects(client, { Bucket: bucket });
            // console.error('===Contents', Contents);
            // const dbData = await getS3Object(client, {
            //     Bucket: bucket,
            //     Key: key,
            // });

            // const response = await getBodyFromResults(dbData);

            // const fetchedData = await compressor.decompress(
            //     response
            // );
        });
    });
});
