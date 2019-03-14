'use strict';

const path = require('path');
const harness = require('@terascope/teraslice-op-test-harness');
const fs = require('fs');
const { remove, ensureDir } = require('fs-extra');
const processor = require('../asset/file_exporter');


const testHarness = harness(processor);
// console.log(testHarness.run)

// Make sure there is a clean output directory for each test cycle. Removes lingering output files
// in the directory or creates the directory if it doesn't exist
async function cleanTestDir() {
    const filepath = getTestFilePath();
    if (fs.existsSync(filepath)) {
        await remove(filepath);
    }
    await ensureDir(filepath);
}

function getTestFilePath(...parts) {
    return path.join(__dirname, 'test_output', ...parts);
}

const data = [
    {
        field1: 42,
        field3: 'test data',
        field2: 55
    },
    {
        field1: 43,
        field3: 'more test data',
        field2: 56
    },
    {
        field1: 44,
        field3: 'even more test data',
        field2: 57
    }
];

const data2 = [
    { data: 'record1' },
    { data: 'record2' },
    { data: 'record3' }
];

const data3 = [
    {
        field1: 42,
        field3: 'test data',
        field2: 55,
        field4: 88
    }
];

const caseMultiFileSpecifyFields = {
    path: getTestFilePath(),
    file_prefix: 'test_',
    format: 'csv',
    line_delimiter: '\n',
    field_delimiter: ',',
    fields: [
        'field3',
        'field1'
    ],
    file_per_slice: true
};

const caseMultiFileAllFields = {
    path: getTestFilePath(),
    file_prefix: 'test_',
    line_delimiter: '\n',
    field_delimiter: ',',
    format: 'csv',
    file_per_slice: true
};

const caseMultiFileAllFieldsHeader = {
    path: getTestFilePath(),
    file_prefix: 'test_',
    line_delimiter: '\n',
    field_delimiter: ',',
    file_per_slice: true,
    format: 'csv',
    include_header: true
};

const caseSingleFileSpecifyFields = {
    path: getTestFilePath(),
    file_prefix: 'test_',
    line_delimiter: '\n',
    field_delimiter: ',',
    format: 'csv',
    fields: [
        'field3',
        'field1'
    ]
};

const caseSingleFileAllFields = {
    path: getTestFilePath(),
    format: 'csv',
    line_delimiter: '\n',
    field_delimiter: ',',
    file_prefix: 'test_'
};

const caseSingleFileAllFieldsHeader = {
    path: getTestFilePath(),
    file_prefix: 'test_',
    line_delimiter: '\n',
    field_delimiter: ',',
    format: 'csv',
    include_header: true
};

// Testing a tab delimiter
const caseTabDelimiter = {
    path: getTestFilePath(),
    file_prefix: 'test_',
    format: 'tsv'
};

// Testing a custom delimiter
const caseCustomDelimiter = {
    path: getTestFilePath(),
    file_prefix: 'test_',
    line_delimiter: '\n',
    field_delimiter: '^',
    format: 'csv'
};

const caseCustomLineDelimiter = {
    path: getTestFilePath(),
    file_prefix: 'test_',
    field_delimiter: ',',
    line_delimiter: '^',
    format: 'csv'
};

const caseldJSON2File = {
    path: getTestFilePath(),
    file_prefix: 'test_',
    line_delimiter: '\n',
    format: 'ldjson'
};

const caseldJSON2FileFields = {
    path: getTestFilePath(),
    file_prefix: 'test_',
    line_delimiter: '\n',
    fields: [
        'field3',
        'field1'
    ],
    format: 'ldjson'
};

const caseJSON2File = {
    path: getTestFilePath(),
    file_prefix: 'test_',
    line_delimiter: '\n',
    format: 'json'
};

const caseRaw2File = {
    path: getTestFilePath(),
    file_prefix: 'test_',
    line_delimiter: '\n',
    format: 'raw'
};

const nodeName = testHarness.context.sysconfig._nodeName;

describe('The file-assets file_exporter', () => {
    beforeEach(() => cleanTestDir());

    it('provides a config schema for use with convict', () => {
        const schema = processor.schema();
        expect(schema.path.default).toEqual(null);
        expect(schema.file_prefix.default).toEqual('export_');
        expect(schema.fields.default).toEqual([]);
        expect(schema.field_delimiter.default).toEqual(',');
        expect(schema.line_delimiter.default).toEqual('\n');
        expect(schema.format.default).toEqual('ldjson');
        expect(schema.file_per_slice.default).toEqual(false);
        expect(schema.include_header.default).toEqual(false);
    });
    it('creates multiple csv files with specific fields', (done) => {
        const opConfig = caseMultiFileSpecifyFields;
        const slices = [data, data];
        return testHarness.runSlices(slices, opConfig)
            .then(() => {
                // This should be equal to three since the test harnes shoves an empty slice through
                expect(fs.readdirSync(getTestFilePath()).length).toEqual(3);
                expect(fs.readFileSync(getTestFilePath(`test_${nodeName}.0`), 'utf-8')).toEqual(
                    '"test data",42\n"more test data",43\n"even more test data",44\n'
                );
                expect(fs.readFileSync(getTestFilePath(`test_${nodeName}.1`), 'utf-8')).toEqual(
                    '"test data",42\n"more test data",43\n"even more test data",44\n'
                );
                done();
            });
    });
    it('creates multiple csv files with all fields', (done) => {
        const opConfig = caseMultiFileAllFields;
        const slices = [data, data];

        return testHarness.runSlices(slices, opConfig)
            .then(() => {
                // This should be equal to three since the test harnes shoves an empty slice through
                expect(fs.readdirSync(getTestFilePath()).length).toEqual(3);
                expect(fs.readFileSync(getTestFilePath(`test_${nodeName}.0`), 'utf-8')).toEqual(
                    '42,"test data",55\n43,"more test data",56\n44,"even more test data",57\n'
                );
                expect(fs.readFileSync(getTestFilePath(`test_${nodeName}.1`), 'utf-8')).toEqual(
                    '42,"test data",55\n43,"more test data",56\n44,"even more test data",57\n'
                );
                done();
            });
    });
    it('creates multiple csv files with all fields and headers', (done) => {
        const opConfig = caseMultiFileAllFieldsHeader;
        const slices = [data, data];
        return testHarness.runSlices(slices, opConfig)
            .then(() => {
                // This should be equal to three since the test harnes shoves an empty slice through
                expect(fs.readdirSync(getTestFilePath()).length).toEqual(3);
                expect(fs.readFileSync(getTestFilePath(`test_${nodeName}.0`), 'utf-8')).toEqual(
                    '"field1","field3","field2"\n'
                    + '42,"test data",55\n'
                    + '43,"more test data",56\n'
                    + '44,"even more test data",57\n'
                );
                expect(fs.readFileSync(getTestFilePath(`test_${nodeName}.1`), 'utf-8')).toEqual(
                    '"field1","field3","field2"\n'
                    + '42,"test data",55\n'
                    + '43,"more test data",56\n'
                    + '44,"even more test data",57\n'
                );
                done();
            });
    });
    it('creates a single csv file with custom fields', (done) => {
        const opConfig = caseSingleFileSpecifyFields;
        const slices = [data, data];
        return testHarness.runSlices(slices, opConfig)
            .then(() => {
                expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
                expect(fs.readFileSync(getTestFilePath(`test_${nodeName}`), 'utf-8')).toEqual(
                    '"test data",42\n'
                    + '"more test data",43\n'
                    + '"even more test data",44\n'
                    + '"test data",42\n'
                    + '"more test data",43\n'
                    + '"even more test data",44\n\n'
                );
                done();
            });
    });
    it('creates a single csv file with all fields', (done) => {
        const opConfig = caseSingleFileAllFields;
        const slices = [data, data];
        return testHarness.runSlices(slices, opConfig)
            .then(() => {
                expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
                expect(fs.readFileSync(getTestFilePath(`test_${nodeName}`), 'utf-8')).toEqual(
                    '42,"test data",55\n'
                    + '43,"more test data",56\n'
                    + '44,"even more test data",57\n'
                    + '42,"test data",55\n'
                    + '43,"more test data",56\n'
                    + '44,"even more test data",57\n\n'
                );
                done();
            });
    });
    it('creates a single csv file and adds a header properly', (done) => {
        const opConfig = caseSingleFileAllFieldsHeader;
        const slices = [data, data];
        return testHarness.runSlices(slices, opConfig)
            .then(() => {
                expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
                // The empty slice does leave an extra newline at the end of the file, but for some
                // reason this check will fail if that is included in the check
                expect(fs.readFileSync(getTestFilePath(`test_${nodeName}`), 'utf-8')).toEqual(
                    '"field1","field3","field2"\n'
                    + '42,"test data",55\n'
                    + '43,"more test data",56\n'
                    + '44,"even more test data",57\n'
                    + '42,"test data",55\n'
                    + '43,"more test data",56\n'
                    + '44,"even more test data",57\n\n'
                );
                done();
            });
    });
    it('creates a single tsv file with a tab delimiter', (done) => {
        const opConfig = caseTabDelimiter;
        const slices = [data];
        return testHarness.runSlices(slices, opConfig)
            .then(() => {
                expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
                expect(fs.readFileSync(getTestFilePath(`test_${nodeName}`), 'utf-8')).toEqual(
                    '42\t"test data"\t55\n'
                    + '43\t"more test data"\t56\n'
                    + '44\t"even more test data"\t57\n\n'
                );
                done();
            });
    });
    it('creates a single csv file with a custom field delimiter', (done) => {
        const opConfig = caseCustomDelimiter;
        const slices = [data];

        return testHarness.runSlices(slices, opConfig)
            .then(() => {
                expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
                expect(fs.readFileSync(getTestFilePath(`test_${nodeName}`), 'utf-8')).toEqual(
                    '42^"test data"^55\n'
                    + '43^"more test data"^56\n'
                    + '44^"even more test data"^57\n\n'
                );
                done();
            });
    });
    it('creates a single csv file with a custom line delimiter', (done) => {
        const opConfig = caseCustomLineDelimiter;
        const slices = [data];
        testHarness.runSlices(slices, opConfig)
            .then(() => {
                expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
                expect(fs.readFileSync(getTestFilePath(`test_${nodeName}`), 'utf-8')).toEqual(
                    '42,"test data",55^'
                    + '43,"more test data",56^'
                    + '44,"even more test data",57^^'
                );
                done();
            });
    });
    it('creates a single file with line-delimite JSON records', (done) => {
        const opConfig = caseldJSON2File;
        const slices = [data];
        return testHarness.runSlices(slices, opConfig)
            .then(() => {
                expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
                expect(fs.readFileSync(getTestFilePath(`test_${nodeName}`), 'utf-8')).toEqual(
                    '{"field1":42,"field3":"test data","field2":55}\n'
                    + '{"field1":43,"field3":"more test data","field2":56}\n'
                    + '{"field1":44,"field3":"even more test data","field2":57}\n'
                );
                done();
            });
    });
    it('filters and orders line-delimited JSON fields', (done) => {
        const opConfig = caseldJSON2FileFields;
        const slices = [data];
        return testHarness.runSlices(slices, opConfig)
            .then(() => {
                expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
                expect(fs.readFileSync(getTestFilePath(`test_${nodeName}`), 'utf-8')).toEqual(
                    '{"field3":"test data","field1":42}\n'
                    + '{"field3":"more test data","field1":43}\n'
                    + '{"field3":"even more test data","field1":44}\n'
                );
                done();
            });
    });
    it('creates single files with a JSON record for `json` format', (done) => {
        const opConfig = caseJSON2File;
        const slices = [data3, data3];
        testHarness.runSlices(slices, opConfig)
            .then(() => {
                expect(fs.readdirSync(getTestFilePath()).length).toEqual(3);
                expect(fs.readFileSync(getTestFilePath(`test_${nodeName}.0`), 'utf-8')).toEqual(
                    '[{"field1":42,"field3":"test data","field2":55,"field4":88}]\n'
                );
                expect(fs.readFileSync(getTestFilePath(`test_${nodeName}.1`), 'utf-8')).toEqual(
                    '[{"field1":42,"field3":"test data","field2":55,"field4":88}]\n'
                );
                done();
            });
    });
    it('creates a single file with raw records on each line', (done) => {
        const opConfig = caseRaw2File;
        const slices = [data2];

        return testHarness.runSlices(slices, opConfig)
            .then(() => {
                expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
                expect(fs.readFileSync(getTestFilePath(`test_${nodeName}`), 'utf-8')).toEqual(
                    'record1\n'
                    + 'record2\n'
                    + 'record3\n'
                );
                done();
            });
    });
});
