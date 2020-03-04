import path from 'path';
import { newTestJobConfig, SlicerTestHarness } from 'teraslice-test-harness';
import { flatten } from '@terascope/job-components';
import { Format } from '../../asset/src/__lib/parser';
// @ts-ignore
const fixtures = require('jest-fixtures');

describe('File slicer json files', () => {
    const slices: any[] = [];

    let harness: SlicerTestHarness;
    let testDataDir: string;

    async function getSlices() {
        const results = await harness.createSlices();
        if (results[0]) {
            slices.push(...results);
            await getSlices();
        }
    }

    beforeEach(async () => {
        testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/json');
        const job = newTestJobConfig({
            analytics: true,
            operations: [
                {
                    _op: 'file_reader',
                    path: testDataDir,
                    size: 750,
                    format: Format.json
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

describe('File slicer non json files', () => {
    const slices: any[] = [];

    let harness: SlicerTestHarness;
    let testDataDir: string;

    beforeEach(async () => {
        testDataDir = await fixtures.copyFixtureIntoTempDir(__dirname, 'file_reader/ldjson');
        const job = newTestJobConfig({
            analytics: true,
            operations: [
                {
                    _op: 'file_reader',
                    path: testDataDir,
                    format: Format.ldjson,
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

    async function getSlices() {
        const results = await harness.createSlices();
        if (results[0]) {
            slices.push(...results);
            await getSlices();
        }
    }

    it('properly slices non-JSON files', async () => {
        await getSlices();
        const flatSlices: any[] = flatten(slices);

        expect(flatSlices.length).toBe(46);
        expect(flatSlices[22].length).toEqual(321);
        expect(flatSlices[45].length).toEqual(321);
    });
});
