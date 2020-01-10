'use strict';

const fixtures = require('jest-fixtures');
const path = require('path');
const {
    newTestJobConfig,
    SlicerTestHarness,
} = require('teraslice-test-harness');

describe('File slicer', () => {
    const slices = [];

    let harness;
    let testDataDir;

    beforeEach(async () => {
        testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/json');
        const job = newTestJobConfig({
            analytics: true,
            operations: [
                {
                    _op: 'file_reader',
                    path: testDataDir,
                    size: 750,
                    format: 'json'
                },
                {
                    _op: 'noop'
                }
            ]
        });
        harness = new SlicerTestHarness(job, {});

        await harness.initialize();
    });
    afterEach(async () => {
        await harness.shutdown();
    });

    it('properly slices JSON files.', async () => {
        async function getSlices() {
            const results = await harness.createSlices();
            if (results[0]) {
                slices.push(results[0]);
                await getSlices();
            }
        }
        await getSlices();
        expect(slices.length).toBe(2);

        // Since slicing happens asynchronously, we need to check which slice has each
        // file. Just need to check the path on one slice.
        if (slices[0].path === path.join(testDataDir, 'array/array.json')) {
            expect(slices[0].length).toEqual(1822);
            expect(slices[1].length).toEqual(364);
        } else {
            expect(slices[1].length).toEqual(1822);
            expect(slices[0].length).toEqual(364);
        }
    });
});

describe('File slicer', () => {
    const slices = [];

    let harness;
    let testDataDir;

    beforeEach(async () => {
        testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/ldjson');
        const job = newTestJobConfig({
            analytics: true,
            operations: [
                {
                    _op: 'file_reader',
                    path: testDataDir,
                    format: 'ldjson',
                    size: 750,
                    line_delimiter: '\n'
                },
                {
                    _op: 'noop'
                }
            ]
        });
        harness = new SlicerTestHarness(job, {});

        await harness.initialize();
    });
    afterEach(async () => {
        await harness.shutdown();
    });

    it('properly slices non-JSON files', async () => {
        async function getSlices() {
            const results = await harness.createSlices();
            if (results[0]) {
                slices.push(results);
                await getSlices();
            }
        }
        await getSlices();
        const flatSlices = [].concat(...slices);
        expect(flatSlices.length).toBe(46);
        expect(flatSlices[22].length).toEqual(321);
        expect(flatSlices[45].length).toEqual(321);
    });
});
