import 'jest-extended';
import { debugLogger } from '@terascope/utils';
// @ts-expect-error
import fixtures from 'jest-fixtures';
import pathModule from 'node:path';
import { fileURLToPath } from 'node:url';
import {
    Compression, FileFetcher, Format,
    ReaderConfig, FileSlice
} from '../../src/index.js';

const dirname = pathModule.dirname(fileURLToPath(import.meta.url));

describe('file-fetcher', () => {
    const logger = debugLogger('file-fetcher');
    let testPath: string;

    function makeFetcher(testConfig: Partial<ReaderConfig> = {}) {
        const config = Object.assign({}, { path: testPath, size: 1000 }, testConfig);
        if (config.format == null) throw new Error('Must provide a format');
        return new FileFetcher(config as ReaderConfig, logger);
    }

    function makePath(pathDir: string, testFile: string) {
        return pathModule.join(pathDir, testFile);
    }

    it('can read ldjson files', async () => {
        const path = await fixtures.copyFixtureIntoTempDir(dirname, 'file_reader/ldjson');
        const slicePath = makePath(path, 'testData.txt');

        const config = {
            path,
            format: Format.ldjson,
            compression: Compression.none
        };

        const fetcher = makeFetcher(config);
        const slice: FileSlice = {
            path: slicePath,
            offset: 0,
            length: 750,
            total: 16820
        };

        const results = await fetcher.read(slice);
        const [record] = results;

        expect(results).toBeArrayOfSize(3);
        expect(record.bytes).toEqual(59696);
    });

    it('can read a json file with a single record', async () => {
        const path = await fixtures.copyFixtureIntoTempDir(dirname, 'file_reader/json/single');
        const slicePath = makePath(path, 'single.json');

        const config = {
            path,
            format: Format.json,
            compression: Compression.none
        };

        const fetcher = makeFetcher(config);
        const slice: FileSlice = {
            path: slicePath,
            offset: 0,
            length: 364,
            total: 364
        };

        const results = await fetcher.read(slice);

        expect(results).toBeArrayOfSize(1);
        expect(results[0].name).toEqual('file_reader');
    });

    it('can read a json file with an array of records', async () => {
        const path = await fixtures.copyFixtureIntoTempDir(dirname, 'file_reader/json/array');
        const slicePath = makePath(path, 'array.json');

        const config = {
            path,
            format: Format.json,
            compression: Compression.none
        };

        const fetcher = makeFetcher(config);
        const slice: FileSlice = {
            path: slicePath,
            offset: 0,
            length: 1822,
            total: 1822
        };

        const results = await fetcher.read(slice);
        const [record] = results;

        expect(results).toBeArrayOfSize(5);
        expect(record.workers).toEqual(10);
    });

    it('can read a csv slice and keeps headers', async () => {
        const path = await fixtures.copyFixtureIntoTempDir(dirname, 'file_reader/csv');
        const slicePath = makePath(path, 'csv.txt');

        const config = {
            path,
            format: Format.csv,
            compression: Compression.none,
            fields: ['data1', 'data2', 'data3', 'data4', 'data5', 'data6'],
            size: 1000,
            remove_header: false,
        };

        const fetcher = makeFetcher(config);
        const slice: FileSlice = {
            path: slicePath,
            offset: 0,
            length: 200,
            total: 1000
        };

        const results = await fetcher.read(slice);
        const [headers, firstRecord] = results;

        expect(results).toBeArrayOfSize(4);
        expect(headers).toEqual({
            data1: 'data1',
            data2: 'data2',
            data3: 'data3',
            data4: 'data4',
            data5: 'data5',
            data6: 'data6'
        });

        expect(firstRecord).toEqual({
            data1: '1',
            data2: '2',
            data3: '3',
            data4: '4',
            data5: '5',
            data6: '6'
        });
    });

    it('can read a csv slice and removes headers', async () => {
        const path = await fixtures.copyFixtureIntoTempDir(dirname, 'file_reader/csv');
        const slicePath = makePath(path, 'csv.txt');

        const config = {
            path,
            format: Format.csv,
            compression: Compression.none,
            fields: ['data1', 'data2', 'data3', 'data4', 'data5', 'data6'],
            size: 1000,
            remove_header: true,
        };

        const fetcher = makeFetcher(config);
        const slice: FileSlice = {
            path: slicePath,
            offset: 0,
            length: 200,
            total: 1000
        };

        const results = await fetcher.read(slice);
        const [firstRecord] = results;

        expect(results).toBeArrayOfSize(3);

        expect(firstRecord).toEqual({
            data1: '1',
            data2: '2',
            data3: '3',
            data4: '4',
            data5: '5',
            data6: '6'
        });
    });

    it('can read a tsv file', async () => {
        const path = await fixtures.copyFixtureIntoTempDir(dirname, 'file_reader/tsv');
        const slicePath = makePath(path, 'tsv.tsv');

        const config = {
            path,
            format: Format.tsv,
            compression: Compression.none,
            fields: ['data1', 'data2', 'data3', 'data4', 'data5', 'data6'],
            size: 1000,
            remove_header: true,
            field_delimiter: '\t',
        };

        const fetcher = makeFetcher(config);
        const slice: FileSlice = {
            path: slicePath,
            offset: 0,
            length: 200,
            total: 1000
        };

        const results = await fetcher.read(slice);
        const [firstRecord] = results;

        expect(results).toBeArrayOfSize(3);

        expect(firstRecord).toEqual({
            data1: '1',
            data2: '2',
            data3: '3',
            data4: '4',
            data5: '5',
            data6: '6'
        });
    });

    it('can read a raw formatted file', async () => {
        const path = await fixtures.copyFixtureIntoTempDir(dirname, 'file_reader/raw');
        const slicePath = makePath(path, 'raw.txt');

        const config = {
            path,
            format: Format.raw,
            compression: Compression.none,
            fields: ['data1', 'data2', 'data3', 'data4', 'data5', 'data6'],
            size: 1000,
        };

        const fetcher = makeFetcher(config);
        const slice: FileSlice = {
            path: slicePath,
            offset: 0,
            length: 200,
            total: 1000
        };

        const results = await fetcher.read(slice);
        const [firstRecord] = results;

        expect(results).toBeArrayOfSize(5);
        expect(firstRecord).toEqual({
            data: 'the quick brown fox jumped over the lazy dog'
        });
    });

    it('can read a compressed file', async () => {
        const path = await fixtures.copyFixtureIntoTempDir(dirname, 't1');
        const slicePath = makePath(path, 'package.json.lz4');

        const config = {
            path,
            format: Format.json,
            file_per_slice: true,
            compression: Compression.lz4,
            size: 1000,
        };

        const fetcher = makeFetcher(config);
        const slice: FileSlice = {
            path: slicePath,
            offset: 0,
            length: 2000,
            total: 2000
        };

        const results = await fetcher.read(slice);
        const [data] = results;

        expect(results).toBeArrayOfSize(1);
        expect(data.version).toEqual('0.1.0');
    });
});
