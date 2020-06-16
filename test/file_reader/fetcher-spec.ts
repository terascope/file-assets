import 'jest-extended';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import path from 'path';

const fixtures = require('jest-fixtures');

describe('File reader\'s fetcher', () => {
    let harness: WorkerTestHarness;

    async function makeTest(config: any) {
        const opConfig = Object.assign({}, { _op: 'file_reader' }, config);
        const job = newTestJobConfig({
            operations: [
                opConfig,
                {
                    _op: 'noop'
                }
            ]
        });

        harness = new WorkerTestHarness(job);

        await harness.initialize();

        return harness;
    }

    it('properly reads an ldjson slice', async () => {
        const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/ldjson/subdir');
        const opConfig = {
            path: testDataDir,
            compression: 'none',
            size: 750,
            format: 'ldjson',
            line_delimiter: '\n'
        };

        const beginningSlice = {
            path: path.join(testDataDir, 'testData.txt'),
            offset: 0,
            length: 750,
            total: 16820
        };

        const test = await makeTest(opConfig);

        const data = await test.runSlice(beginningSlice);
        // 3 JSON records in the test ldjson file
        expect(data).toBeArrayOfSize(3);
    // increase the timeout for this one
    }, 10000);

    it('properly reads a json file with a single record', async () => {
        // Using some test data generated by the TS `elasticsearch_data_generator`
        const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/json/single');
        const opConfig = {
            _op: 'file_reader',
            path: testDataDir,
            compression: 'none',
            size: 400,
            format: 'json',
            line_delimiter: '\n'
        };

        const beginningSlice = {
            path: path.join(testDataDir, 'single.json'),
            offset: 0,
            length: 364,
            total: 364
        };

        const test = await makeTest(opConfig);

        const data = await test.runSlice(beginningSlice);

        expect(data.length).toEqual(1);
    });

    it('properly reads a json file with an array of records', async () => {
        // Using some test data generated by the TS `elasticsearch_data_generator`
        const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/json/array');
        const opConfig = {
            _op: 'file_reader',
            path: testDataDir,
            compression: 'none',
            size: 4000,
            format: 'json',
            line_delimiter: '\n'
        };

        const beginningSlice = {
            path: path.join(testDataDir, 'array.json'),
            offset: 0,
            length: 1822,
            total: 1822
        };

        const test = await makeTest(opConfig);

        const data = await test.runSlice(beginningSlice);

        expect(data.length).toEqual(5);
    });

    it('properly reads a csv slice and keeps headers', async () => {
        // Using some test data generated by the TS `elasticsearch_data_generator`
        const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/csv');
        const opConfig = {
            _op: 'file_reader',
            path: testDataDir,
            compression: 'none',
            size: 1000,
            fields: ['data1', 'data2', 'data3', 'data4', 'data5', 'data6'],
            format: 'csv',
            field_delimiter: ',',
            remove_header: false,
            line_delimiter: '\n'
        };

        const beginningSlice = {
            path: path.join(testDataDir, 'csv.txt'),
            offset: 0,
            length: 200,
            total: 1000
        };

        const test = await makeTest(opConfig);

        const data = await test.runSlice(beginningSlice);

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

        const opConfig = {
            _op: 'file_reader',
            path: testDataDir,
            compression: 'none',
            size: 1000,
            fields: ['data1', 'data2', 'data3', 'data4', 'data5', 'data6'],
            format: 'csv',
            field_delimiter: ',',
            remove_header: true,
            line_delimiter: '\n'
        };

        const beginningSlice = {
            path: path.join(testDataDir, 'csv.txt'),
            offset: 0,
            length: 200,
            total: 1000
        };

        const test = await makeTest(opConfig);

        const data = await test.runSlice(beginningSlice);

        expect(data.length).toEqual(3);
        expect(data[0]).toEqual({
            data1: '1', data2: '2', data3: '3', data4: '4', data5: '5', data6: '6'
        });
    });

    it('properly reads a tsv slice and coerces field delimiter', async () => {
        // Using some test data generated by the TS `elasticsearch_data_generator`
        const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/tsv');
        const opConfig = {
            _op: 'file_reader',
            path: testDataDir,
            compression: 'none',
            size: 1000,
            fields: ['data1', 'data2', 'data3', 'data4', 'data5', 'data6'],
            format: 'tsv',
            field_delimiter: 'chillywilly',
            remove_header: true,
            line_delimiter: '\n'
        };

        const beginningSlice = {
            path: path.join(testDataDir, 'tsv.tsv'),
            offset: 0,
            length: 200,
            total: 1000
        };

        const test = await makeTest(opConfig);

        const data = await test.runSlice(beginningSlice);

        expect(data.length).toEqual(3);
        expect(data[0]).toEqual({
            data1: '1', data2: '2', data3: '3', data4: '4', data5: '5', data6: '6'
        });
    });

    it('properly reads a raw slice', async () => {
        // Using some test data generated by the TS `elasticsearch_data_generator`
        const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/raw');

        const opConfig = {
            _op: 'file_reader',
            path: testDataDir,
            compression: 'none',
            size: 200,
            format: 'raw',
            line_delimiter: '\n'
        };

        const beginningSlice = {
            path: path.join(testDataDir, 'raw.txt'),
            offset: 0,
            length: 200,
            total: 1000
        };

        const test = await makeTest(opConfig);

        const data = await test.runSlice(beginningSlice);
        // The raw text is added as an attribute on the record since the data entity is
        // a record
        expect(data[0].data).toEqual('the quick brown fox jumped over the lazy dog');
        expect(data.length).toEqual(5);
    });
});
