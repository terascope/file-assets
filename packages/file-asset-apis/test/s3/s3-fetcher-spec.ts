import 'jest-extended';
import { debugLogger, toString } from '@terascope/core-utils';
import {
    makeClient, cleanupBucket, upload,
    UploadConfig
} from './helpers.js';
import {
    Compression, Format, ReaderConfig,
    S3Fetcher, createS3Bucket, FileSlice,
} from '../../src/index.js';

describe('S3 Fetcher API', () => {
    const logger = debugLogger('s3-sender');
    const id = 'some-id';
    const bucket = 's3-fetcher-api';
    const dirPath = '/testing/';
    const path = `${bucket}${dirPath}`;
    let client: any;

    beforeAll(async () => {
        client = await makeClient();
        await cleanupBucket(client, bucket);
        // make sure bucket exists
        await createS3Bucket(client, { Bucket: bucket });
    });

    afterAll(async () => {
        if (client) {
            await cleanupBucket(client, bucket);
        }
    });

    it('can fetch ldjson S3 data', async () => {
        const testData = [{ some: 'data' }, { other: 'data' }];
        const format = Format.ldjson;
        const compression = Compression.none;
        const testDirPath = 'ldjson_test';
        const testPath = `${path}/${testDirPath}`;

        const uploadConfig: UploadConfig = {
            format,
            id,
            path,
            bucket,
            compression,
            sliceCount: 0
        };
        const fileName = await upload(client, uploadConfig, testData.slice());

        const config: ReaderConfig = {
            path: testPath,
            format,
            compression,
            file_per_slice: true,
            size: 1000
        };

        const fetcher = new S3Fetcher(client, config, logger);
        const slice: FileSlice = {
            path: fileName,
            offset: 0,
            length: 200,
            total: 1000
        };

        const results = await fetcher.read(slice);

        expect(results).toBeArrayOfSize(2);
        expect(results).toEqual(testData);
    });

    it('can fetch json S3 data', async () => {
        const testData = { other: 'data' };
        const format = Format.json;
        const compression = Compression.none;
        const testDirPath = 'json_test';
        const testPath = `${path}/${testDirPath}`;

        const config: ReaderConfig = {
            path: testPath,
            format,
            compression,
            file_per_slice: true,
            size: 1000
        };

        const uploadConfig: UploadConfig = {
            format,
            id,
            path,
            bucket,
            compression,
            sliceCount: 0
        };
        const fileName = await upload(client, uploadConfig, [testData]);
        const fetcher = new S3Fetcher(client, config, logger);
        const slice: FileSlice = {
            path: fileName,
            offset: 0,
            length: 200,
            total: 1000
        };

        const results = await fetcher.read(slice);

        expect(results).toBeArrayOfSize(1);
        expect(results).toEqual([testData]);
    });

    it('can fetch raw S3 data', async () => {
        const testData = [{ data: 'some data' }];
        const format = Format.raw;
        const compression = Compression.none;
        const testDirPath = 'raw_test';
        const testPath = `${path}/${testDirPath}`;

        const config: ReaderConfig = {
            path: testPath,
            format,
            compression,
            file_per_slice: true,
            size: 1000
        };

        const uploadConfig: UploadConfig = {
            format,
            id,
            path,
            bucket,
            compression,
            sliceCount: 0
        };
        const fileName = await upload(client, uploadConfig, testData);
        const fetcher = new S3Fetcher(client, config, logger);
        const slice: FileSlice = {
            path: fileName,
            offset: 0,
            length: 200,
            total: 1000
        };

        const results = await fetcher.read(slice);

        expect(results).toBeArrayOfSize(1);
        expect(results).toEqual(testData);
    });

    it('can fetch csv S3 data', async () => {
        const testData = [
            {
                car: 'Audi',
                price: 40000,
                color: 'blue'
            },
            {
                car: 'BMW',
                price: 35000,
                color: 'black'
            },
            {
                car: 'Porsche',
                price: 60000,
                color: 'green'
            }
        ];
        const format = Format.csv;
        const compression = Compression.none;
        const testDirPath = 'csv_test';
        const testPath = `${path}/${testDirPath}`;

        const config: ReaderConfig = {
            path: testPath,
            format,
            compression,
            file_per_slice: true,
            size: 1000
        };

        const uploadConfig: UploadConfig = {
            format,
            id,
            path,
            bucket,
            compression,
            sliceCount: 0
        };
        const fileName = await upload(client, uploadConfig, testData);
        const fetcher = new S3Fetcher(client, config, logger);
        const slice: FileSlice = {
            path: fileName,
            offset: 0,
            length: 200,
            total: 1000
        };

        const results = await fetcher.read(slice);

        expect(results).toBeArrayOfSize(3);

        const cars = results.map((obj) => obj.field1);
        const price = results.map((obj) => obj.field2);
        const color = results.map((obj) => obj.field3);

        testData.forEach((record) => {
            expect(cars).toContain(record.car);
            expect(price).toContain(toString(record.price));
            expect(color).toContain(record.color);
        });
    });

    it('can fetch tsv S3 data', async () => {
        const testData = [
            {
                car: 'Audi',
                price: 40000,
                color: 'blue'
            },
            {
                car: 'BMW',
                price: 35000,
                color: 'black'
            },
            {
                car: 'Porsche',
                price: 60000,
                color: 'green'
            }
        ];
        const format = Format.tsv;
        const compression = Compression.none;
        const testDirPath = 'tsv_test';
        const testPath = `${path}/${testDirPath}`;

        const config = {
            path: testPath,
            format,
            compression,
            file_per_slice: true,
            size: 1000,
            field_delimiter: '\t'
        } as ReaderConfig;

        const uploadConfig = {
            format,
            id,
            path,
            bucket,
            compression,
            sliceCount: 0,
            field_delimiter: '\t'
        } as UploadConfig;
        const fileName = await upload(client, uploadConfig, testData);
        const fetcher = new S3Fetcher(client, config, logger);
        const slice: FileSlice = {
            path: fileName,
            offset: 0,
            length: 200,
            total: 1000
        };

        const results = await fetcher.read(slice);

        expect(results).toBeArrayOfSize(3);

        const cars = results.map((obj) => obj.field1);
        const price = results.map((obj) => obj.field2);
        const color = results.map((obj) => obj.field3);

        testData.forEach((record) => {
            expect(cars).toContain(record.car);
            expect(price).toContain(toString(record.price));
            expect(color).toContain(record.color);
        });
    });

    it('can fetch gzip compressed S3 data', async () => {
        const testData = [{ some: 'data' }, { other: 'data' }];
        const format = Format.ldjson;
        const compression = Compression.gzip;
        const testDirPath = 'ldjson_test';
        const testPath = `${path}/${testDirPath}`;

        const uploadConfig: UploadConfig = {
            format,
            id,
            path,
            bucket,
            compression,
            sliceCount: 0
        };
        const fileName = await upload(client, uploadConfig, testData.slice());

        const config: ReaderConfig = {
            path: testPath,
            format,
            compression,
            file_per_slice: true,
            size: 1000
        };

        const fetcher = new S3Fetcher(client, config, logger);
        const slice: FileSlice = {
            path: fileName,
            offset: 0,
            length: 200,
            total: 1000
        };

        const results = await fetcher.read(slice);

        expect(results).toBeArrayOfSize(2);
        expect(results).toEqual(testData);
    });

    it('can fetch lz4 compressed S3 data', async () => {
        const testData = [{ some: 'data' }, { other: 'data' }];
        const format = Format.ldjson;
        const compression = Compression.lz4;
        const testDirPath = 'ldjson_test';
        const testPath = `${path}/${testDirPath}`;

        const uploadConfig: UploadConfig = {
            format,
            id,
            path,
            bucket,
            compression,
            sliceCount: 0
        };
        const fileName = await upload(client, uploadConfig, testData.slice());

        const config: ReaderConfig = {
            path: testPath,
            format,
            compression,
            file_per_slice: true,
            size: 1000
        };

        const fetcher = new S3Fetcher(client, config, logger);
        const slice: FileSlice = {
            path: fileName,
            offset: 0,
            length: 200,
            total: 1000
        };

        const results = await fetcher.read(slice);

        expect(results).toBeArrayOfSize(2);
        expect(results).toEqual(testData);
    });
});
