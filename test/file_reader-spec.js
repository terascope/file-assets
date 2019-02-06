'use strict';

// const FileDB = require('../asset/compressed_file_reader/filedb');
const fixtures = require('jest-fixtures');
const { debugLogger } = require('@terascope/utils');
const { newReader, newSlicer } = require('../asset/file_reader/index.js');

const logger = debugLogger('file_reader-spec');

describe('The file_reader', () => {
    describe('reader', () => {
        it('properly reads a slice', async (done) => {
            // Using some test data generated by the TS `elasticsearch_data_generator`
            const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/subdir');
            // Only thing needed in the opConfig by the reader is the read size
            const opConfig = {
                size: 750,
                format: 'json',
                delimiter: '\n'
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
    });
    describe('slicer', () => {
        it('properly slices a file', async () => {
            const slices = [];
            const testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader');
            const executionContext = {
                config: {
                    operations: [
                        {
                            path: testDataDir,
                            size: 750,
                            format: 'json',
                            delimiter: '\n'
                        }
                    ]
                }
            };
            // Setup a slicer and extract all of the slices generated. If there are less than 46
            // slices, the slicer did not properly load the subdirectory
            const slicer = await newSlicer(undefined, executionContext, undefined, logger);
            for (let i = 0; i < 46; i += 1) {
                slices.push(slicer[0]());
            }
            // Given the size of the two test files, the last slice for the first file should be #22
            // and be shorter than 750 bytes
            expect(slices[22].length).toEqual(321);
            expect(slices[45].length).toEqual(321);
        });
    });
});
