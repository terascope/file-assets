import { debugLogger } from '@terascope/utils';
import { Format } from '../../asset/src/__lib/parser';
import { getOffsets, getChunk, _averageRecordSize } from '../../asset/src/__lib/chunked-file-reader';

// Mock logger
const logger = debugLogger('chunked-file-reader');

// Mock reader
function Reader(reads: string[]) {
    return async () => reads.shift();
}

const ldjsonOpConfig = {
    line_delimiter: '\n',
    format: Format.ldjson,
    _dead_letter_action: 'none'
};

const multiCharOpConfig = {
    line_delimiter: '\r\n\f',
    format: Format.ldjson,
    _dead_letter_action: 'none'
};

const rawOpConfig = {
    line_delimiter: '\n',
    format: Format.raw,
    _dead_letter_action: 'none'
};

const metadata = {
    path: '/test/file'
};

const jsonOpConfig = {
    format: Format.json,
    _dead_letter_action: 'none'
};

const tsvOpConfig = {
    format: Format.tsv,
    field_delimiter: '\t',
    fields: ['data1', 'data2', 'data3', 'data4', 'data5'],
    _dead_letter_action: 'none'
};

const csvOpConfig = {
    format: Format.csv,
    remove_header: true,
    field_delimiter: ',',
    line_delimiter: '\n',
    fields: ['data1', 'data2', 'data3', 'data4', 'data5'],
    _dead_letter_action: 'none'

};

describe('The chunked file reader', () => {
    // Test with a check to see if `getMetadata()` works for a record
    it('returna a DataEntity for JSON data.', async () => {
        const slice = { offset: 100, length: 5, total: 30 };
        const reader = Reader([
            '\n{"test4": "data"}\n{"test5": data}\n{"test6": "data"}\n',
        ]);

        const [data] = await getChunk(reader, slice, ldjsonOpConfig, logger, metadata);

        expect(data.getMetadata().path).toEqual('/test/file');
    });

    // Test with a check to see if `getMetadata()` works for a record
    it('returna a DataEntity for `raw` data.', async () => {
        const slice = { offset: 100, length: 5, total: 30 };
        const reader = Reader([
            '\n{"test4": "data"}\n{"test5": data}\n{"test6": "data"}\n',
        ]);

        const [data] = await getChunk(reader, slice, rawOpConfig, logger, metadata);

        expect(data.getMetadata().path).toEqual('/test/file');
    });

    it('does not let one bad JSON record spoil the bunch.', async () => {
        const slice = { offset: 100, length: 5, total: 30 };
        const expected = [{ test4: 'data' }, { test6: 'data' }];

        const reader = Reader([
            '\n{"test4": "data"}\n{"test5": data}\n{"test6": "data"}\n',
        ]);

        const data = await getChunk(reader, slice, ldjsonOpConfig, logger, metadata);

        expect(data).toEqual(expected);
    });

    it('supports multi-character delimiters.', async () => {
        const slice = { offset: 0, length: 5, total: 30 };
        const expected = [{ test4: 'data' }, { test5: 'data' }];

        const reader = Reader([
            '{"test4": "data"}\r\n\f{"test5": "data"}\r\n\f',
        ]);

        const data = await getChunk(reader, slice, multiCharOpConfig, logger, metadata);
        expect(data).toEqual(expected);
    });

    it('handles a start slice and margin collection.', async () => {
        const slice = { offset: 0, length: 5, total: 30 };
        const expected = [{ test1: 'data' }, { test2: 'data' }, { test3: 'data' }];

        const reader = Reader([
            '{"test1": "data"}\n{"test2": "data"}\n{"test3": "data"',
            '}\n{"test4": "data"}\n',
        ]);

        const data = await getChunk(reader, slice, ldjsonOpConfig, logger, metadata);
        expect(data).toEqual(expected);
    });

    it('handles reading margin until delimiter found.', async () => {
        const slice = { offset: 0, length: 5, total: 112 };
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

        const reader = Reader([
            '{"t1":"d"}\n{"t2":"d"}\n{"t3":"d", ',
            '"x": "abcdefghijklmnopqrstuz", ',
            '"y": "ABCDEFGHIJKLMNOPQRSTUZ", ',
            '"z": 123456789}\n',
        ]);

        const data = await getChunk(reader, slice, ldjsonOpConfig, logger, metadata);
        expect(data).toEqual(expected);
    });

    it('handles reading margin when delimiter never found.', async () => {
        const slice = { offset: 0, length: 5, total: 95 };
        const expected = [{ t1: 'd' }, { t2: 'd' }];

        const reader = Reader([
            '{"t1":"d"}\n{"t2":"d"}\n{"t3":"d", ',
            '"x": "abcdefghijklmnopqrstuz", ',
            '"y": "ABCDEFGHIJKLMNOPQRSTUZ", ',
        ]);

        const data = await getChunk(reader, slice, ldjsonOpConfig, logger, metadata);
        expect(data).toEqual(expected);
    });

    it('handles a middle slice with a complete record and no margin.', async () => {
        const slice = { offset: 10, length: 10, total: 30 };
        const expected = [{ test4: 'data' }, { test5: 'data' }, { test6: 'data' }];

        const reader = Reader([
            '\n{"test4": "data"}\n{"test5": "data"}\n{"test6": "data"}\n',
        ]);

        const data = await getChunk(reader, slice, ldjsonOpConfig, logger, metadata);
        expect(data).toEqual(expected);
    });

    it('handles an end slice starting with a partial record.', async () => {
        const slice = { offset: 20, length: 10, total: 30 };
        const expected = [{ test7: 'data' }, { test8: 'data' }];

        const reader = Reader([
            '{"test6": "data"}\n{"test7": "data"}\n{"test8": "data"}\n',
        ]);

        const data = await getChunk(reader, slice, ldjsonOpConfig, logger, metadata);
        expect(data).toEqual(expected);
    });

    it('handes an array of JSON records.', async () => {
        const slice = { offset: 0, length: 30, total: 30 };
        const expected = [{ test6: 'data' }, { test7: 'data' }, { test8: 'data' }];

        const reader = Reader([
            '[{"test6": "data"},{"test7": "data"},{"test8": "data"}]',
        ]);

        const data = await getChunk(reader, slice, jsonOpConfig, logger, metadata);
        expect(data).toEqual(expected);
    });

    it('handes a JSON record.', async () => {
        const slice = { offset: 0, length: 30, total: 30 };
        const expected = [{ testData: { test6: 'data', test7: 'data', test8: 'data' } }];

        const reader = Reader([
            '{"testData":{"test6":"data","test7":"data","test8":"data"}}',
        ]);

        const data = await getChunk(reader, slice, jsonOpConfig, logger, metadata);
        expect(data).toEqual(expected);
    });

    it('handes TSV input.', async () => {
        const slice = { offset: 0, length: 30, total: 30 };
        const expected = [{
            data1: '42', data2: '43', data3: '44', data4: '45', data5: '46'
        }];

        const reader = Reader([
            '42\t43\t44\t45\t46',
        ]);

        const data = await getChunk(reader, slice, tsvOpConfig, logger, metadata);
        expect(data).toEqual(expected);
    });

    it('handes CSV input and removes headers.', async () => {
        const slice = { offset: 0, length: 30, total: 30 };
        const expected = [{
            data1: '42', data2: '43', data3: '44', data4: '45', data5: '46'
        }];

        const reader = Reader([
            'data1,data2,data3,data4,data5\n42,43,44,45,46',
        ]);

        const data = await getChunk(reader, slice, csvOpConfig, logger, metadata);
        expect(data).toEqual(expected);
    });

    it('accurately calculates average record size.', () => {
        const records = [
            '{"test1":"data"}',
            '{"test2":"data"}',
            '{"test3":"data"}',
            '{"test4":"data"}',
            '{"test5":"data"}',
            '{"test6":"data"}',
            '{"test7":"data"}',
            '{"test8":"data"}'
        ];

        const avgSize = _averageRecordSize(records);
        expect(avgSize).toEqual(16);
    });

    it('computes offsets', () => {
        expect(getOffsets(10, 0, '\n')).toEqual([]);

        expect(getOffsets(10, 9, '\n')).toEqual([
            { offset: 0, length: 9 },
        ]);

        expect(getOffsets(10, 10, '\n')).toEqual([
            { offset: 0, length: 10 },
        ]);

        expect(getOffsets(10, 15, '\n')).toEqual([
            { offset: 0, length: 10 },
            { offset: 9, length: 6 },
        ]);

        expect(getOffsets(10, 20, '\n')).toEqual([
            { offset: 0, length: 10 },
            { offset: 9, length: 11 },
        ]);

        expect(getOffsets(10, 20, '\r\n')).toEqual([
            { offset: 0, length: 10 },
            { offset: 8, length: 12 },
        ]);

        expect(getOffsets(10, 21, '\r\n')).toEqual([
            { offset: 0, length: 10 },
            { offset: 8, length: 12 },
            { offset: 18, length: 3 },
        ]);
    });
});
