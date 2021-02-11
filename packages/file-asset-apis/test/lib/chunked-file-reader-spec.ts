import 'jest-extended';
import { debugLogger, DataEntity } from '@terascope/job-components';
import {
    ChunkedFileReader, Format, ChunkedConfig, Compression
} from '../../src';

// Mock logger
const logger = debugLogger('chunked-file-reader');

class Test extends ChunkedFileReader {
    data: string[];

    constructor(config: ChunkedConfig, data: string[]) {
        super(config, logger);
        this.data = data;
    }

    async fetch(_msg: any): Promise<string> {
        const data = this.data.shift();
        return data as string;
    }
}

function makeConfig(config: any) {
    const defaults = {
        line_delimiter: '\n',
        format: Format.ldjson,
        _dead_letter_action: 'none',
        compression: Compression.none
    };
    return Object.assign({}, defaults, config) as ChunkedConfig;
}

const ldjsonOpConfig = makeConfig({
    format: Format.ldjson,
});

const multiCharOpConfig = makeConfig({
    line_delimiter: '\r\n\f',
    format: Format.ldjson,
});

const rawOpConfig = makeConfig({
    format: Format.raw,
});

const jsonOpConfig = makeConfig({
    format: Format.json,
});

const tsvOpConfig = makeConfig({
    format: Format.tsv,
    field_delimiter: '\t',
    fields: ['data1', 'data2', 'data3', 'data4', 'data5'],
});

const csvOpConfig = makeConfig({
    format: Format.csv,
    remove_header: true,
    field_delimiter: ',',
    fields: ['data1', 'data2', 'data3', 'data4', 'data5'],
});

describe('The chunked file reader', () => {
    // Test with a check to see if `getMetadata()` works for a record
    it('return a DataEntity for JSON data.', async () => {
        const slice = {
            offset: 100, length: 5, total: 30, path: '/test/file'
        };
        const incData = [
            '\n{"test4": "data"}\n{"test5": data}\n{"test6": "data"}\n',
        ];

        const test = new Test(ldjsonOpConfig, incData);

        const results = await test.read(slice);
        expect(results).toBeArray();
        const data = results[0];
        expect(DataEntity.isDataEntity(data)).toBeTrue();
        const metaData = data.getMetadata();
        expect(metaData.path).toEqual('/test/file');
    });

    // // Test with a check to see if `getMetadata()` works for a record
    it('return a DataEntity for `raw` data.', async () => {
        const slice = {
            offset: 100, length: 5, total: 30, path: '/test/file'
        };
        const incData = [
            '\n{"test4": "data"}\n{"test5": data}\n{"test6": "data"}\n',
        ];

        const test = new Test(rawOpConfig, incData);
        const results = await test.read(slice);

        expect(results).toBeDefined();
        const data = results[0];
        expect(DataEntity.isDataEntity(data)).toBeTrue();
        const metaData = data.getMetadata();
        expect(metaData.path).toEqual('/test/file');
    });

    it('does not let one bad JSON record spoil the bunch.', async () => {
        const slice = {
            offset: 100, length: 5, total: 30, path: '/test/file'
        };
        const expected = [{ test4: 'data' }, { test6: 'data' }];

        const incData = [
            '\n{"test4": "data"}\n{"test5": data}\n{"test6": "data"}\n',
        ];

        const test = new Test(ldjsonOpConfig, incData);
        const results = await test.read(slice);

        expect(results).toEqual(expected);
    });

    it('supports multi-character delimiters.', async () => {
        const slice = {
            offset: 0, length: 5, total: 30, path: '/test/file'
        };
        const expected = [{ test4: 'data' }, { test5: 'data' }];

        const incData = [
            '{"test4": "data"}\r\n\f{"test5": "data"}\r\n\f',
        ];

        const test = new Test(multiCharOpConfig, incData);
        const results = await test.read(slice);

        expect(results).toEqual(expected);
    });

    it('handles a start slice and margin collection.', async () => {
        const slice = {
            offset: 0, length: 5, total: 30, path: '/test/file'
        };
        const expected = [{ test1: 'data' }, { test2: 'data' }, { test3: 'data' }];

        const incData = [
            '{"test1": "data"}\n{"test2": "data"}\n{"test3": "data"',
            '}\n{"test4": "data"}\n',
        ];

        const test = new Test(ldjsonOpConfig, incData);
        const results = await test.read(slice);

        expect(results).toEqual(expected);
    });

    it('handles reading margin until delimiter found.', async () => {
        const slice = {
            offset: 0, length: 5, total: 112, path: '/test/file'
        };
        const expected = [
            { t1: 'd' },
            { t2: 'd' },
            {
                t3: 'd',
                x: 'abcdefghijklmnopqrstuz',
                y: 'ABCDEFGHIJKLMNOPQRSTUZ',
                z: 123456789,
            }
        ];

        const incData = [
            '{"t1":"d"}\n{"t2":"d"}\n{"t3":"d", ',
            '"x": "abcdefghijklmnopqrstuz", ',
            '"y": "ABCDEFGHIJKLMNOPQRSTUZ", ',
            '"z": 123456789}\n',
        ];

        const test = new Test(ldjsonOpConfig, incData);
        const results = await test.read(slice);

        expect(results).toEqual(expected);
    });

    it('handles reading margin when delimiter never found.', async () => {
        const slice = {
            offset: 0, length: 5, total: 95, path: '/test/file'
        };
        const expected = [{ t1: 'd' }, { t2: 'd' }];

        const incData = [
            '{"t1":"d"}\n{"t2":"d"}\n{"t3":"d", ',
            '"x": "abcdefghijklmnopqrstuz", ',
            '"y": "ABCDEFGHIJKLMNOPQRSTUZ", ',
        ];

        const test = new Test(ldjsonOpConfig, incData);
        const results = await test.read(slice);

        expect(results).toEqual(expected);
    });

    it('handles a middle slice with a complete record and no margin.', async () => {
        const slice = {
            offset: 10, length: 10, total: 30, path: '/test/file'
        };
        const expected = [{ test4: 'data' }, { test5: 'data' }, { test6: 'data' }];

        const incData = [
            '\n{"test4": "data"}\n{"test5": "data"}\n{"test6": "data"}\n',
        ];

        const test = new Test(ldjsonOpConfig, incData);
        const results = await test.read(slice);

        expect(results).toEqual(expected);
    });

    it('handles an end slice starting with a partial record.', async () => {
        const slice = {
            offset: 20, length: 10, total: 30, path: '/test/file'
        };
        const expected = [{ test7: 'data' }, { test8: 'data' }];

        const incData = [
            '{"test6": "data"}\n{"test7": "data"}\n{"test8": "data"}\n',
        ];

        const test = new Test(ldjsonOpConfig, incData);
        const results = await test.read(slice);

        expect(results).toEqual(expected);
    });

    it('handles an array of JSON records.', async () => {
        const slice = {
            offset: 0, length: 30, total: 30, path: '/test/file'
        };
        const expected = [{ test6: 'data' }, { test7: 'data' }, { test8: 'data' }];

        const incData = [
            '[{"test6": "data"},{"test7": "data"},{"test8": "data"}]',
        ];

        const test = new Test(jsonOpConfig, incData);
        const results = await test.read(slice);

        expect(results).toEqual(expected);
    });

    it('handles a JSON record.', async () => {
        const slice = {
            offset: 0, length: 30, total: 30, path: '/test/file'
        };
        const expected = [{ testData: { test6: 'data', test7: 'data', test8: 'data' } }];

        const incData = [
            '{"testData":{"test6":"data","test7":"data","test8":"data"}}',
        ];

        const test = new Test(jsonOpConfig, incData);
        const results = await test.read(slice);

        expect(results).toEqual(expected);
    });

    it('handles TSV input.', async () => {
        const slice = {
            offset: 0, length: 30, total: 30, path: '/test/file'
        };
        const expected = [{
            data1: '42', data2: '43', data3: '44', data4: '45', data5: '46'
        }];

        const incData = [
            '42\t43\t44\t45\t46',
        ];

        const test = new Test(tsvOpConfig, incData);
        const results = await test.read(slice);

        expect(results).toEqual(expected);
    });

    it('handles CSV input and removes headers.', async () => {
        const slice = {
            offset: 0, length: 30, total: 30, path: '/test/file'
        };
        const expected = [{
            data1: '42', data2: '43', data3: '44', data4: '45', data5: '46'
        }];

        const incData = [
            'data1,data2,data3,data4,data5\n42,43,44,45,46',
        ];

        const test = new Test(csvOpConfig, incData);
        const results = await test.read(slice);

        expect(results).toEqual(expected);
    });
});
