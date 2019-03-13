'use strict';

// const FileDB = require('../asset/compressed_file_reader/filedb');
const fixtures = require('jest-fixtures');
const path = require('path');
const { debugLogger } = require('@terascope/utils');
const { TestContext } = require('@terascope/job-components');
const { newReader, newSlicer, schema } = require('../asset/file_reader/index.js');

const logger = debugLogger('file_reader-spec');

describe('The file_reader', () => {
    it('provides a config schema for use with convict', () => {
        const schemaVals = schema();
        expect(schemaVals.path.default).toEqual(null);
        expect(schemaVals.size.default).toEqual(10000000);
        expect(schemaVals.fields.default).toEqual([]);
        expect(schemaVals.field_delimiter.default).toEqual(',');
        expect(schemaVals.line_delimiter.default).toEqual('\n');
        expect(schemaVals.format.default).toEqual('ldjson');
        expect(schemaVals.remove_header.default).toEqual(true);
    });
    describe('reader', () => {
        it('properly reads an ldjson slice', async (done) => {
            // Using some test data generated by the TS `elasticsearch_data_generator`
            const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/ldjson/subdir');
            // Only thing needed in the opConfig by the reader is the read size
            const opConfig = {
                size: 750,
                format: 'ldjson',
                line_delimiter: '\n'
            };
            // `context` is not needed, so it can just be left undefined
            const sliceProcessor = newReader(undefined, opConfig);
            const beginningSlice = {
                path: `${testDataDir}/testData.txt`,
                offset: 0,
                length: 750,
                total: 16820
            };
            const readData = await sliceProcessor(beginningSlice, logger);
            expect(readData.length).toEqual(3);
            done();
        });
        it('properly reads a json file with a single record', async (done) => {
            // Using some test data generated by the TS `elasticsearch_data_generator`
            const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/json/single');
            // Only thing needed in the opConfig by the reader is the read size
            const opConfig = {
                size: 400,
                format: 'json'
            };
            // `context` is not needed, so it can just be left undefined
            const sliceProcessor = newReader(undefined, opConfig);
            const beginningSlice = {
                path: `${testDataDir}/single.json`,
                offset: 0,
                length: 364,
                total: 364
            };
            const readData = await sliceProcessor(beginningSlice, logger);
            expect(readData.length).toEqual(1);
            done();
        });
        it('properly reads a json file with an array of records', async (done) => {
            // Using some test data generated by the TS `elasticsearch_data_generator`
            const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/json/array');
            // Need to define size since there's no config schema check
            const opConfig = {
                size: 4000,
                format: 'json',
                line_delimiter: '\n'
            };
            // `context` is not needed, so it can just be left undefined
            const sliceProcessor = newReader(undefined, opConfig);
            const beginningSlice = {
                path: `${testDataDir}/array.json`,
                offset: 0,
                length: 1822,
                total: 1822
            };
            const readData = await sliceProcessor(beginningSlice, logger);
            expect(readData.length).toEqual(5);
            done();
        });
        it('properly reads a csv slice and keeps headers', async (done) => {
            // Using some test data generated by the TS `elasticsearch_data_generator`
            const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/csv');
            // Only thing needed in the opConfig by the reader is the read size
            const opConfig = {
                size: 1000,
                fields: ['data1', 'data2', 'data3', 'data4', 'data5', 'data6'],
                format: 'csv',
                field_delimiter: ',',
                remove_header: false,
                line_delimiter: '\n'
            };
            // `context` is not needed, so it can just be left undefined
            const sliceProcessor = newReader(undefined, opConfig);
            const beginningSlice = {
                path: `${testDataDir}/csv.txt`,
                offset: 0,
                length: 200,
                total: 1000
            };
            const readData = await sliceProcessor(beginningSlice, logger);
            expect(readData.length).toEqual(4);
            expect(readData[0]).toEqual({
                data1: 'data1', data2: 'data2', data3: 'data3', data4: 'data4', data5: 'data5', data6: 'data6'
            });
            expect(readData[1]).toEqual({
                data1: '1', data2: '2', data3: '3', data4: '4', data5: '5', data6: '6'
            });
            done();
        });
        it('properly reads a csv slice and removes headers', async (done) => {
            // Using some test data generated by the TS `elasticsearch_data_generator`
            const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/csv');
            // Only thing needed in the opConfig by the reader is the read size
            const opConfig = {
                size: 1000,
                fields: ['data1', 'data2', 'data3', 'data4', 'data5', 'data6'],
                format: 'csv',
                field_delimiter: ',',
                remove_header: true,
                line_delimiter: '\n'
            };
            // `context` is not needed, so it can just be left undefined
            const sliceProcessor = newReader(undefined, opConfig);
            const beginningSlice = {
                path: `${testDataDir}/csv.txt`,
                offset: 0,
                length: 200,
                total: 1000
            };
            const readData = await sliceProcessor(beginningSlice, logger);
            expect(readData.length).toEqual(4);
            expect(readData[0]).toEqual(null);
            expect(readData[1]).toEqual({
                data1: '1', data2: '2', data3: '3', data4: '4', data5: '5', data6: '6'
            });
            done();
        });
        it('properly reads a tsv slice and coerces field delimiter', async (done) => {
            // Using some test data generated by the TS `elasticsearch_data_generator`
            const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/tsv');
            // Only thing needed in the opConfig by the reader is the read size
            const opConfig = {
                size: 1000,
                fields: ['data1', 'data2', 'data3', 'data4', 'data5', 'data6'],
                format: 'tsv',
                field_delimiter: 'chillywilly',
                remove_header: true,
                line_delimiter: '\n'
            };
            // `context` is not needed, so it can just be left undefined
            const sliceProcessor = newReader(undefined, opConfig);
            const beginningSlice = {
                path: `${testDataDir}/tsv.tsv`,
                offset: 0,
                length: 200,
                total: 1000
            };
            const readData = await sliceProcessor(beginningSlice, logger);
            expect(readData.length).toEqual(4);
            expect(readData[0]).toEqual(null);
            expect(readData[1]).toEqual({
                data1: '1', data2: '2', data3: '3', data4: '4', data5: '5', data6: '6'
            });
            done();
        });
        it('properly reads a raw slice', async (done) => {
            // Using some test data generated by the TS `elasticsearch_data_generator`
            const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/raw');
            // Only thing needed in the opConfig by the reader is the read size
            const opConfig = {
                size: 200,
                format: 'raw',
                line_delimiter: '\n'
            };
            // `context` is not needed, so it can just be left undefined
            const sliceProcessor = newReader(undefined, opConfig);
            const beginningSlice = {
                path: `${testDataDir}/raw.txt`,
                offset: 0,
                length: 200,
                total: 1000
            };
            const readData = await sliceProcessor(beginningSlice, logger);
            // The raw text is added as an attribute on the record since the data entity is a record
            expect(readData[0].data).toEqual('the quick brown fox jumped over the lazy dog');
            expect(readData.length).toEqual(5);
            done();
        });
    });
    describe('slicer', () => {
        it('properly slices a non-json file', async (done) => {
            const slices = [];
            const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/ldjson');
            const executionContext = {
                config: {
                    operations: [
                        {
                            path: testDataDir,
                            size: 750,
                            line_delimiter: '\n'
                        }
                    ]
                }
            };
            // Setup a slicer and extract all of the slices generated. If there are less than 46
            // slices, the slicer did not properly load the subdirectory
            const context = new TestContext('file_reader');
            const slicer = newSlicer(context, executionContext, undefined, logger);
            // Delay a bit to let the slice load the dummy files
            const sliceWatcher = setInterval(() => {
                const currentSlice = slicer[0]();
                if (currentSlice) slices.push(currentSlice);
                if (currentSlice === null) {
                    clearInterval(sliceWatcher);
                    expect(slices[22].length).toEqual(321);
                    expect(slices[45].length).toEqual(321);
                    done();
                }
            }, 60);
        });
        it('properly slices a json file', async (done) => {
            const slices = [];
            const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/json');
            // Setting the size here should have no impact on the actual slice sizes. If it did,
            // this test would fail
            const executionContext = {
                config: {
                    operations: [
                        {
                            path: testDataDir,
                            size: 750,
                            format: 'json'
                        }
                    ]
                }
            };
            // Setup a slicer and extract all of the slices generated. If there are less than 46
            // slices, the slicer did not properly load the subdirectory
            const context = new TestContext('file_reader');
            const slicer = newSlicer(context, executionContext, undefined, logger);
            // Delay a bit to let the slice load the dummy files
            const sliceWatcher = setInterval(() => {
                const currentSlice = slicer[0]();
                if (currentSlice) slices.push(currentSlice);
                if (currentSlice === null) {
                    clearInterval(sliceWatcher);
                    // Two files => two slices
                    expect(slices.length).toEqual(2);
                    // Since slicing happens asynchronously, we need to check which slice has each
                    // file. Just need to check the path on one slice.
                    if (slices[0].path === path.join(testDataDir, 'array/array.json')) {
                        expect(slices[0].length).toEqual(1822);
                        expect(slices[1].length).toEqual(364);
                    } else {
                        expect(slices[1].length).toEqual(1822);
                        expect(slices[0].length).toEqual(364);
                    }
                    done();
                }
            }, 60);
        });
    });
});
