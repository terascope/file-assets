'use strict';

const harness = require('@terascope/teraslice-op-test-harness');
const fs = require('fs');
const processor = require('../asset/csv_exporter');


const testHarness = harness(processor);

// Make sure there is a clean output directory for each test cycle. Removes lingering output files
// in the directory or creates the directory if it doesn't exist
function cleanTestDir() {
    if (fs.readdirSync('./spec').indexOf('test_output') > -1) {
        if (fs.readdirSync('./spec/test_output').length > 0) {
            fs.readdirSync('./spec/test_output').forEach(file => fs.unlinkSync(`./spec/test_output/${file}`));
        }
    } else {
        fs.mkdirSync('./spec/test_output');
    }
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

const caseMultiFileSpecifyFields = {
    _op: 'csv_exporter',
    path: './spec/test_output',
    file_prefix: 'test',
    fields: [
        'field3',
        'field1'
    ],
    file_per_slice: true
};

const caseMultiFileAllFields = {
    _op: 'csv_exporter',
    path: './spec/test_output',
    file_prefix: 'test',
    file_per_slice: true
};

const caseMultiFileAllFieldsHeader = {
    _op: 'csv_exporter',
    path: './spec/test_output',
    file_prefix: 'test',
    file_per_slice: true,
    include_header: true
};

const caseSingleFileSpecifyFields = {
    _op: 'csv_exporter',
    path: './spec/test_output',
    file_prefix: 'test',
    fields: [
        'field3',
        'field1'
    ]
};

const caseSingleFileAllFields = {
    _op: 'csv_exporter',
    path: './spec/test_output',
    file_prefix: 'test'
};

const caseSingleFileAllFieldsHeader = {
    _op: 'csv_exporter',
    path: './spec/test_output',
    file_prefix: 'test',
    include_header: true
};

// Testing a tab delimiter specifically
const caseCustomDelimiter = {
    _op: 'csv_exporter',
    path: './spec/test_output',
    file_prefix: 'test',
    delimiter: '\t'
};

// const metricPayload = harness.run(data, opConfig);

describe('The file-assets csv_exporter processor', () => {
    it('provides a config schema for use with convict', () => {
        const schema = processor.schema();
        expect(schema.path.default).toEqual(null);
        expect(schema.file_prefix.default).toEqual('export');
        expect(schema.fields.default).toEqual([]);
        expect(schema.delimiter.default).toEqual(',');
        expect(schema.file_per_slice.default).toEqual(false);
        expect(schema.include_header.default).toEqual(false);
    });
    cleanTestDir();
    it('creates multiple files with specific fields', () => {
        testHarness.run(data, caseMultiFileSpecifyFields);
        testHarness.run(data, caseMultiFileSpecifyFields);
        expect(fs.readdirSync('./spec/test_output').length).toEqual(2);
        const file0 = fs.readFileSync('./spec/test_output/test_undefined.0', 'utf-8');
        const file1 = fs.readFileSync('./spec/test_output/test_undefined.1', 'utf-8');
        expect(file0).toEqual(
            '"test data",42\n"more test data",43\n"even more test data",44\n'
        );
        expect(file1).toEqual(
            '"test data",42\n"more test data",43\n"even more test data",44\n'
        );
        cleanTestDir();
    });
    it('creates multiple files with all fields', () => {
        testHarness.run(data, caseMultiFileAllFields);
        testHarness.run(data, caseMultiFileAllFields);
        expect(fs.readdirSync('./spec/test_output').length).toEqual(2);
        const file0 = fs.readFileSync('./spec/test_output/test_undefined.0', 'utf-8');
        const file1 = fs.readFileSync('./spec/test_output/test_undefined.1', 'utf-8');
        expect(file0).toEqual(
            '42,"test data",55\n43,"more test data",56\n44,"even more test data",57\n'
        );
        expect(file1).toEqual(
            '42,"test data",55\n43,"more test data",56\n44,"even more test data",57\n'
        );
        cleanTestDir();
    });
    it('creates multiple files with all fields and headers', () => {
        testHarness.run(data, caseMultiFileAllFieldsHeader);
        testHarness.run(data, caseMultiFileAllFieldsHeader);
        expect(fs.readdirSync('./spec/test_output').length).toEqual(2);
        const file0 = fs.readFileSync('./spec/test_output/test_undefined.0', 'utf-8');
        const file1 = fs.readFileSync('./spec/test_output/test_undefined.1', 'utf-8');
        expect(file0).toEqual(
            '"field1","field3","field2"\n'
            + '42,"test data",55\n'
            + '43,"more test data",56\n'
            + '44,"even more test data",57\n'
        );
        expect(file1).toEqual(
            '"field1","field3","field2"\n'
            + '42,"test data",55\n'
            + '43,"more test data",56\n'
            + '44,"even more test data",57\n'
        );
        cleanTestDir();
    });
    it('creates a single file with custom fields', () => {
        testHarness.run(data, caseSingleFileSpecifyFields);
        testHarness.run(data, caseSingleFileSpecifyFields);
        expect(fs.readdirSync('./spec/test_output').length).toEqual(1);
        const file = fs.readFileSync('./spec/test_output/test_undefined', 'utf-8');
        expect(file).toEqual(
            '"test data",42\n'
            + '"more test data",43\n'
            + '"even more test data",44\n'
            + '"test data",42\n'
            + '"more test data",43\n'
            + '"even more test data",44\n'
        );
        cleanTestDir();
    });
    it('creates a single file with all fields', () => {
        testHarness.run(data, caseSingleFileAllFields);
        testHarness.run(data, caseSingleFileAllFields);
        expect(fs.readdirSync('./spec/test_output').length).toEqual(1);
        const file = fs.readFileSync('./spec/test_output/test_undefined', 'utf-8');
        expect(file).toEqual(
            '42,"test data",55\n'
            + '43,"more test data",56\n'
            + '44,"even more test data",57\n'
            + '42,"test data",55\n'
            + '43,"more test data",56\n'
            + '44,"even more test data",57\n'
        );
        cleanTestDir();
    });
    it('creates a single file and adds a header properly', () => {
        testHarness.run(data, caseSingleFileAllFieldsHeader);
        testHarness.run(data, caseSingleFileAllFieldsHeader);
        expect(fs.readdirSync('./spec/test_output').length).toEqual(1);
        const file = fs.readFileSync('./spec/test_output/test_undefined', 'utf-8');
        expect(file).toEqual(
            '"field1","field3","field2"\n'
            + '42,"test data",55\n'
            + '43,"more test data",56\n'
            + '44,"even more test data",57\n'
            + '42,"test data",55\n'
            + '43,"more test data",56\n'
            + '44,"even more test data",57\n'
        );
        cleanTestDir();
    });
    it('creates a single file with a custom delimiter', () => {
        testHarness.run(data, caseCustomDelimiter);
        expect(fs.readdirSync('./spec/test_output').length).toEqual(1);
        const file = fs.readFileSync('./spec/test_output/test_undefined', 'utf-8');
        expect(file).toEqual(
            '42\t"test data"\t55\n'
            + '43\t"more test data"\t56\n'
            + '44\t"even more test data"\t57\n'
        );
        cleanTestDir();
    });
});
