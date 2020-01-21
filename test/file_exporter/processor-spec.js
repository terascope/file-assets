'use strict';

const { TestContext } = require('@terascope/job-components');
const { DataEntity } = require('@terascope/utils');
const path = require('path');
const fs = require('fs');
const { remove, ensureDir } = require('fs-extra');
const Processor = require('../../asset/file_exporter/processor');

function getTestFilePath(...parts) {
    return path.join(__dirname, 'test_output/test', ...parts);
}

async function cleanTestDir() {
    const filepath = getTestFilePath();
    if (fs.existsSync(filepath)) {
        await remove(filepath);
    }
    await ensureDir(filepath);
}

describe('File exporter processor', () => {
    beforeEach(() => cleanTestDir());

    const context = new TestContext('file-reader');
    // This sets up the opconfig for the first test
    const processor = new Processor(context,
        {
            _op: 'file_exporter',
            path: `${getTestFilePath()}`,
            compression: 'none',
            format: 'csv',
            line_delimiter: '\n',
            field_delimiter: ',',
            fields: [
                'field3',
                'field1'
            ],
            file_per_slice: true,
            include_header: false
        },
        {
            name: 'file_exporter'
        });

    const data = [
        DataEntity.make(
            {
                field1: 42,
                field3: 'test data',
                field2: 55
            }
        ),
        DataEntity.make({
            field1: 43,
            field3: 'more test data',
            field2: 56
        }),
        DataEntity.make({
            field1: 44,
            field3: 'even more test data',
            field2: 57
        })
    ];

    const data2 = [
        DataEntity.make({ data: 'record1' }),
        DataEntity.make({ data: 'record2' }),
        DataEntity.make({ data: 'record3' })
    ];

    const data3 = [DataEntity.make(
        {
            field1: 42,
            field3: 'test data',
            field2: 55,
            field4: 88
        }
    )];

    const complexData = [
        DataEntity.make(
            {
                field1: {
                    subfield1: 22,
                    subfield2: 44
                },
                field2: 66
            }
        ),
        DataEntity.make({
            field1: [
                {
                    subfield1: 22,
                    subfield2: 44
                }
            ],
            field2: 66
        })
    ];

    const emptySlice = [DataEntity.make(null)];

    beforeAll(async () => {
        await processor.initialize();
    });

    afterAll(async () => {
        await processor.shutdown();
        context.apis.foundation.getSystemEvents().removeAllListeners();
    });

    it('creates multiple CSV files with specific fields', async () => {
        const nodeName = processor.worker;
        await processor.onBatch(data);
        await processor.onBatch(data);
        // expect(processor.toEqual(1))
        expect(fs.readdirSync(getTestFilePath()).length).toEqual(2);
        expect(fs.readFileSync(getTestFilePath(`${nodeName}.0`), 'utf-8')).toEqual(
            '"test data",42\n"more test data",43\n"even more test data",44\n'
        );
        expect(fs.readFileSync(getTestFilePath(`${nodeName}.1`), 'utf-8')).toEqual(
            '"test data",42\n"more test data",43\n"even more test data",44\n'
        );
    });
    it('ignores empty slices', async () => {
        await processor.onBatch(emptySlice);
        // expect(processor.toEqual(1))
        expect(fs.readdirSync(getTestFilePath()).length).toEqual(0);
    });
    it('creates multiple CSV files with all fields', async () => {
        const nodeName = processor.worker;
        // reset the slice count and CSV options since this is a "new" job
        processor.sliceCount = -1;
        processor.csvOptions.fields = null;
        await processor.onBatch(data);
        await processor.onBatch(data);
        expect(fs.readdirSync(getTestFilePath()).length).toEqual(2);
        expect(fs.readFileSync(getTestFilePath(`${nodeName}.0`), 'utf-8')).toEqual(
            '42,"test data",55\n43,"more test data",56\n44,"even more test data",57\n'
        );
        expect(fs.readFileSync(getTestFilePath(`${nodeName}.1`), 'utf-8')).toEqual(
            '42,"test data",55\n43,"more test data",56\n44,"even more test data",57\n'
        );
    });
    it('creates multiple CSV files with all fields and headers', async () => {
        const nodeName = processor.worker;
        // reset the slice count and CSV options since this is a "new" job
        processor.sliceCount = -1;
        processor.csvOptions.fields = null;
        processor.csvOptions.header = true;
        await processor.onBatch(data);
        await processor.onBatch(data);
        expect(fs.readdirSync(getTestFilePath()).length).toEqual(2);
        expect(fs.readFileSync(getTestFilePath(`${nodeName}.0`), 'utf-8')).toEqual(
            '"field1","field3","field2"\n'
            + '42,"test data",55\n'
            + '43,"more test data",56\n'
            + '44,"even more test data",57\n'
        );
        expect(fs.readFileSync(getTestFilePath(`${nodeName}.1`), 'utf-8')).toEqual(
            '"field1","field3","field2"\n'
            + '42,"test data",55\n'
            + '43,"more test data",56\n'
            + '44,"even more test data",57\n'
        );
    });
    it('creates a single csv file with custom fields', async () => {
        const nodeName = processor.worker;
        processor.sliceCount = -1;
        processor.csvOptions.fields = [
            'field3',
            'field1'
        ];
        processor.csvOptions.header = false;
        processor.opConfig.file_per_slice = false;
        await processor.onBatch(data);
        await processor.onBatch(data);
        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(nodeName), 'utf-8')).toEqual(
            '"test data",42\n'
            + '"more test data",43\n'
            + '"even more test data",44\n'
            + '"test data",42\n'
            + '"more test data",43\n'
            + '"even more test data",44\n'
        );
    });
    it('creates a single csv file with custom complex fields', async () => {
        const nodeName = processor.worker;
        processor.sliceCount = -1;
        processor.csvOptions.fields = [
            'field2',
            'field1'
        ];
        processor.csvOptions.header = false;
        processor.opConfig.file_per_slice = false;
        await processor.onBatch(complexData);
        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(nodeName), 'utf-8')).toEqual(
            '66,"{""subfield1"":22,""subfield2"":44}"\n'
            + '66,"[{""subfield1"":22,""subfield2"":44}]"\n'
        );
    });
    it('creates a single csv file with all fields', async () => {
        const nodeName = processor.worker;
        processor.sliceCount = -1;
        processor.csvOptions.fields = null;
        processor.opConfig.file_per_slice = false;
        await processor.onBatch(data);
        await processor.onBatch(data);
        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(nodeName), 'utf-8')).toEqual(
            '42,"test data",55\n'
            + '43,"more test data",56\n'
            + '44,"even more test data",57\n'
            + '42,"test data",55\n'
            + '43,"more test data",56\n'
            + '44,"even more test data",57\n'
        );
    });
    it('creates a single csv file and adds a header properly', async () => {
        const nodeName = processor.worker;
        processor.sliceCount = -1;
        processor.csvOptions.fields = null;
        processor.csvOptions.header = true;
        processor.opConfig.file_per_slice = false;
        await processor.onBatch(data);
        await processor.onBatch(data);
        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(nodeName), 'utf-8')).toEqual(
            '"field1","field3","field2"\n'
            + '42,"test data",55\n'
            + '43,"more test data",56\n'
            + '44,"even more test data",57\n'
            + '42,"test data",55\n'
            + '43,"more test data",56\n'
            + '44,"even more test data",57\n'
        );
    });
    it('creates a single tsv file with a tab delimiter', async () => {
        const nodeName = processor.worker;
        processor.sliceCount = -1;
        processor.csvOptions.delimiter = '\t';
        processor.csvOptions.header = false;
        processor.opConfig.file_per_slice = false;
        await processor.onBatch(data);
        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(nodeName), 'utf-8')).toEqual(
            '42\t"test data"\t55\n'
            + '43\t"more test data"\t56\n'
            + '44\t"even more test data"\t57\n'
        );
    });
    it('creates a single csv file with a custom delimiter', async () => {
        const nodeName = processor.worker;
        processor.sliceCount = -1;
        processor.csvOptions.delimiter = '^';
        processor.csvOptions.header = false;
        await processor.onBatch(data);
        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(nodeName), 'utf-8')).toEqual(
            '42^"test data"^55\n'
            + '43^"more test data"^56\n'
            + '44^"even more test data"^57\n'
        );
    });
    it('creates a single csv file with a custom line delimiter', async () => {
        const nodeName = processor.worker;
        processor.sliceCount = -1;
        processor.csvOptions.delimiter = ',';
        processor.csvOptions.header = false;
        processor.csvOptions.eol = '^';
        processor.opConfig.line_delimiter = '^';
        await processor.onBatch(data);
        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(nodeName), 'utf-8')).toEqual(
            '42,"test data",55^'
            + '43,"more test data",56^'
            + '44,"even more test data",57^'
        );
    });
    it('creates a single file with line-delimited JSON records', async () => {
        const nodeName = processor.worker;
        processor.sliceCount = -1;
        processor.opConfig.line_delimiter = '\n';
        processor.opConfig.fields = [];
        processor.opConfig.format = 'ldjson';
        await processor.onBatch(data);
        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(nodeName), 'utf-8')).toEqual(
            '{"field1":42,"field3":"test data","field2":55}\n'
            + '{"field1":43,"field3":"more test data","field2":56}\n'
            + '{"field1":44,"field3":"even more test data","field2":57}\n'
        );
    });
    it('filters and orders line-delimited JSON fields', async () => {
        const nodeName = processor.worker;
        processor.sliceCount = -1;
        processor.opConfig.fields = [
            'field3',
            'field1'
        ];
        await processor.onBatch(data);
        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(nodeName), 'utf-8')).toEqual(
            '{"field3":"test data","field1":42}\n'
            + '{"field3":"more test data","field1":43}\n'
            + '{"field3":"even more test data","field1":44}\n'
        );
    });
    it('filters and line-delimited JSON fields with nested objects', async () => {
        const nodeName = processor.worker;
        processor.sliceCount = -1;
        processor.opConfig.fields = [
            'field2',
            'field1',
            'subfield1',
            'subfield2'
        ];
        await processor.onBatch(complexData);
        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(nodeName), 'utf-8')).toEqual(
            '{"field2":66,"field1":{"subfield1":22,"subfield2":44}}\n'
            + '{"field2":66,"field1":[{"subfield1":22,"subfield2":44}]}\n'
        );
    });
    it('creates a single file with a JSON record for `json` format', async () => {
        const nodeName = processor.worker;
        processor.sliceCount = -1;
        processor.opConfig.file_per_slice = true;
        processor.opConfig.format = 'json';
        await processor.onBatch(data3);
        await processor.onBatch(data3);
        expect(fs.readdirSync(getTestFilePath()).length).toEqual(2);
        expect(fs.readFileSync(getTestFilePath(`${nodeName}.0`), 'utf-8')).toEqual(
            '[{"field1":42,"field3":"test data","field2":55,"field4":88}]\n'
        );
        expect(fs.readFileSync(getTestFilePath(`${nodeName}.1`), 'utf-8')).toEqual(
            '[{"field1":42,"field3":"test data","field2":55,"field4":88}]\n'
        );
    });
    it('ignores non-existant fields in ldjson', async () => {
        const nodeName = processor.worker;
        processor.sliceCount = -1;
        processor.opConfig.file_per_slice = true;
        processor.opConfig.format = 'ldjson';
        processor.opConfig.fields = ['field1', 'field2', 'field8'];
        await processor.onBatch(data3);
        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(`${nodeName}.0`), 'utf-8')).toEqual(
            '{"field1":42,"field2":55}\n'
        );
    });
    it('creates a single file wiht raw records on each line', async () => {
        const nodeName = processor.worker;
        processor.sliceCount = -1;
        processor.opConfig.file_per_slice = false;
        processor.opConfig.format = 'raw';
        await processor.onBatch(data2);
        expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
        expect(fs.readFileSync(getTestFilePath(nodeName), 'utf-8')).toEqual(
            'record1\n'
            + 'record2\n'
            + 'record3\n'
        );
    });
    it('coerces field delimiter to \\t for TSV output', () => {
        const newProcessor = new Processor(context,
            {
                _op: 'file_exporter',
                path: 'chillywilly',
                format: 'tsv',
                field_delimiter: ',',
                fields: []
            },
            {
                name: 'file_exporter'
            });
        expect(newProcessor.csvOptions.delimiter).toEqual('\t');
    });
});
