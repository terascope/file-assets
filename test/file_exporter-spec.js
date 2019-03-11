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

// Not sure how to get context in with the teraslice-op-test-harness. Assumption is that the
// worker's name comes from context.sysconfig._nodeName based on code from the HDFS file chunker

// const context = {
//     foundation: {
//     },
//     sysconfig: {
//         _nodeName: 'ts-node-1'
//     }
// };

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
    'record1',
    'record2',
    'record3'
];

const caseMultiFileSpecifyFields = {
    path: getTestFilePath(),
    file_prefix: 'test',
    format: 'csv',
    fields: [
        'field3',
        'field1'
    ],
    file_per_slice: true
};

const caseMultiFileAllFields = {
    path: getTestFilePath(),
    file_prefix: 'test',
    format: 'csv',
    file_per_slice: true
};

const caseMultiFileAllFieldsHeader = {
    path: getTestFilePath(),
    file_prefix: 'test',
    file_per_slice: true,
    format: 'csv',
    include_header: true
};

const caseSingleFileSpecifyFields = {
    path: getTestFilePath(),
    file_prefix: 'test',
    format: 'csv',
    fields: [
        'field3',
        'field1'
    ]
};

const caseSingleFileAllFields = {
    path: getTestFilePath(),
    format: 'csv',
    file_prefix: 'test'
};

const caseSingleFileAllFieldsHeader = {
    path: getTestFilePath(),
    file_prefix: 'test',
    format: 'csv',
    include_header: true
};

// Testing a tab delimiter
const caseTabDelimiter = {
    path: getTestFilePath(),
    file_prefix: 'test',
    format: 'tsv'
};

// Testing a custom delimiter
const caseCustomDelimiter = {
    path: getTestFilePath(),
    file_prefix: 'test',
    delimiter: '^',
    format: 'csv'
};

const caseJSON2File = {
    path: getTestFilePath(),
    file_prefix: 'test',
    format: 'json'
};

const caseJSON2FileFields = {
    path: getTestFilePath(),
    file_prefix: 'test',
    fields: [
        'field3',
        'field1'
    ],
    format: 'json'
};

const caseText2File = {
    path: getTestFilePath(),
    file_prefix: 'test',
    format: 'text'
};

const nodeName = testHarness.context.sysconfig._nodeName;

// const metricPayload = harness.run(data, opConfig);

describe('The file-assets csv_exporter processor', () => {
    beforeEach(() => cleanTestDir());

    it('provides a config schema for use with convict', () => {
        const schema = processor.schema();
        expect(schema.path.default).toEqual(null);
        expect(schema.file_prefix.default).toEqual('export');
        expect(schema.fields.default).toEqual([]);
        expect(schema.delimiter.default).toEqual(',');
        expect(schema.format.default).toEqual('json');
        expect(schema.file_per_slice.default).toEqual(false);
        expect(schema.include_header.default).toEqual(false);
    });

    it('creates multiple files with specific fields', () => {
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
            });
    });

    it('creates multiple files with all fields', () => {
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
            });
    });
    it('creates multiple files with all fields and headers', () => {
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
            });
    });

    it('creates a single file with custom fields', () => {
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
            });
    });

    it('creates a single file with all fields', () => {
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
            });
    });

    it('creates a single file and adds a header properly', () => {
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
            });
    });

    it('creates a single file with a tab delimiter', () => {
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
            });
    });

    it('creates a single file with a Custom delimiter', () => {
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
            });
    });

    it('creates a single file with JSON records on each line', () => {
        const opConfig = caseJSON2File;
        const slices = [data];
        return testHarness.runSlices(slices, opConfig)
            .then(() => {
                expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
                expect(fs.readFileSync(getTestFilePath(`test_${nodeName}`), 'utf-8')).toEqual(
                    '{"field1":42,"field3":"test data","field2":55}\n'
                    + '{"field1":43,"field3":"more test data","field2":56}\n'
                    + '{"field1":44,"field3":"even more test data","field2":57}\n'
                );
            });
    });

    it('filters and orders JSON fields', () => {
        const opConfig = caseJSON2FileFields;
        const slices = [data];
        return testHarness.runSlices(slices, opConfig)
            .then(() => {
                expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
                expect(fs.readFileSync(getTestFilePath(`test_${nodeName}`), 'utf-8')).toEqual(
                    '{"field3":"test data","field1":42}\n'
                    + '{"field3":"more test data","field1":43}\n'
                    + '{"field3":"even more test data","field1":44}\n'
                );
            });
    });

    it('creates a single file with text records on each line', () => {
        const opConfig = caseText2File;
        const slices = [data2];

        return testHarness.runSlices(slices, opConfig)
            .then(() => {
                expect(fs.readdirSync(getTestFilePath()).length).toEqual(1);
                expect(fs.readFileSync(getTestFilePath(`test_${nodeName}`), 'utf-8')).toEqual(
                    'record1\n'
                    + 'record2\n'
                    + 'record3\n'
                );
            });
    });
});
