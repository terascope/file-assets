'use strict';

const { TestContext } = require('@terascope/job-components');
const fixtures = require('jest-fixtures');
const path = require('path');
const Fetcher = require('../../asset/file_reader/fetcher');


describe('File reader\'s fetcher', () => {
    it('properly reads an ldjson slice', async () => {
        const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/ldjson/subdir');
        const context = new TestContext('file-reader');
        const fetcher = new Fetcher(context,
            {
                _op: 'file_reader',
                path: testDataDir,
                size: 750,
                format: 'ldjson',
                line_delimiter: '\n'
            },
            {
                name: 's3_exporter'
            });
        const beginningSlice = {
            path: path.join(testDataDir, 'testData.txt'),
            offset: 0,
            length: 750,
            total: 16820
        };
        const data = await fetcher.fetch(beginningSlice);
        // 3 JSON records in the test ldjson file
        expect(data.length).toEqual(3);
    });

    it('properly reads a json file with a single record', async () => {
        // Using some test data generated by the TS `elasticsearch_data_generator`
        const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/json/single');
        // Only thing needed in the opConfig by the reader is the read size
        const context = new TestContext('file-reader');
        const fetcher = new Fetcher(context,
            {
                _op: 'file_reader',
                path: testDataDir,
                size: 400,
                format: 'json',
                line_delimiter: '\n'
            },
            {
                name: 's3_exporter'
            });
        const beginningSlice = {
            path: path.join(testDataDir, 'single.json'),
            offset: 0,
            length: 364,
            total: 364
        };
        const data = await fetcher.fetch(beginningSlice);
        // 3 JSON records in the test ldjson file
        expect(data.length).toEqual(1);
    });
    it('properly reads a json file with an array of records', async () => {
        // Using some test data generated by the TS `elasticsearch_data_generator`
        const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/json/array');
        const context = new TestContext('file-reader');
        const fetcher = new Fetcher(context,
            {
                _op: 'file_reader',
                path: testDataDir,
                size: 4000,
                format: 'json',
                line_delimiter: '\n'
            },
            {
                name: 's3_exporter'
            });
        // `context` is not needed, so it can just be left undefined
        const beginningSlice = {
            path: path.join(testDataDir, 'array.json'),
            offset: 0,
            length: 1822,
            total: 1822
        };
        const data = await fetcher.fetch(beginningSlice);
        // 3 JSON records in the test ldjson file
        expect(data.length).toEqual(5);
    });
    it('properly reads a csv slice and keeps headers', async () => {
        // Using some test data generated by the TS `elasticsearch_data_generator`
        const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/csv');
        const context = new TestContext('file-reader');
        const fetcher = new Fetcher(context,
            {
                _op: 'file_reader',
                path: testDataDir,
                size: 1000,
                fields: ['data1', 'data2', 'data3', 'data4', 'data5', 'data6'],
                format: 'csv',
                field_delimiter: ',',
                remove_header: false,
                line_delimiter: '\n'
            },
            {
                name: 's3_exporter'
            });
        const beginningSlice = {
            path: path.join(testDataDir, 'csv.txt'),
            offset: 0,
            length: 200,
            total: 1000
        };
        const data = await fetcher.fetch(beginningSlice);
        expect(data.length).toEqual(4);
        expect(data[0]).toEqual({
            data1: 'data1', data2: 'data2', data3: 'data3', data4: 'data4', data5: 'data5', data6: 'data6'
        });
        expect(data[1]).toEqual({
            data1: '1', data2: '2', data3: '3', data4: '4', data5: '5', data6: '6'
        });
    });
    it('properly reads a csv slice and removes headers', async () => {
        // Using some test data generated by the TS `elasticsearch_data_generator`
        const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/csv');
        // Using some test data generated by the TS `elasticsearch_data_generator`
        const context = new TestContext('file-reader');
        const fetcher = new Fetcher(context,
            {
                _op: 'file_reader',
                path: testDataDir,
                size: 1000,
                fields: ['data1', 'data2', 'data3', 'data4', 'data5', 'data6'],
                format: 'csv',
                field_delimiter: ',',
                remove_header: true,
                line_delimiter: '\n'
            },
            {
                name: 's3_exporter'
            });
        const beginningSlice = {
            path: path.join(testDataDir, 'csv.txt'),
            offset: 0,
            length: 200,
            total: 1000
        };
        const data = await fetcher.fetch(beginningSlice);
        expect(data.length).toEqual(3);
        expect(data[0]).toEqual({
            data1: '1', data2: '2', data3: '3', data4: '4', data5: '5', data6: '6'
        });
    });
    it('properly reads a tsv slice and coerces field delimiter', async () => {
        // Using some test data generated by the TS `elasticsearch_data_generator`
        const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/tsv');
        const context = new TestContext('file-reader');
        const fetcher = new Fetcher(context,
            {
                _op: 'file_reader',
                path: testDataDir,
                size: 1000,
                fields: ['data1', 'data2', 'data3', 'data4', 'data5', 'data6'],
                format: 'tsv',
                field_delimiter: 'chillywilly',
                remove_header: true,
                line_delimiter: '\n'
            },
            {
                name: 's3_exporter'
            });
        const beginningSlice = {
            path: path.join(testDataDir, 'tsv.tsv'),
            offset: 0,
            length: 200,
            total: 1000
        };
        const data = await fetcher.fetch(beginningSlice);
        expect(data.length).toEqual(3);
        expect(data[0]).toEqual({
            data1: '1', data2: '2', data3: '3', data4: '4', data5: '5', data6: '6'
        });
    });
    it('properly reads a raw slice', async () => {
        // Using some test data generated by the TS `elasticsearch_data_generator`
        const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/raw');
        const context = new TestContext('file-reader');
        const fetcher = new Fetcher(context,
            {
                _op: 'file_reader',
                path: testDataDir,
                size: 200,
                format: 'raw',
                line_delimiter: '\n'
            },
            {
                name: 's3_exporter'
            });
        const beginningSlice = {
            path: path.join(testDataDir, 'raw.txt'),
            offset: 0,
            length: 200,
            total: 1000
        };
        const data = await fetcher.fetch(beginningSlice);
        // The raw text is added as an attribute on the record since the data entity is
        // a record
        expect(data[0].data).toEqual('the quick brown fox jumped over the lazy dog');
        expect(data.length).toEqual(5);
    });
});
