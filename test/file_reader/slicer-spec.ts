import 'jest-extended';
import path from 'path';
import { newTestJobConfig, SlicerTestHarness } from 'teraslice-test-harness';
import { flatten, SliceRequest } from '@terascope/job-components';
import { Format } from '@terascope/file-asset-apis';

const fixtures = require('jest-fixtures');

describe('File slicer json files', () => {
    let harness: SlicerTestHarness;
    let testDataDir: string;
    let slices: (SliceRequest|null)[];

    beforeAll(async () => {
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

        slices = await harness.getAllSlices() as (SliceRequest|null)[];
    });

    afterAll(async () => {
        await harness.shutdown();
    });

    it('should create 3 slices (one null)', async () => {
        expect(slices).toBeArrayOfSize(3);
        expect(slices).toContain(null);
    });

    it('should properly slice the single json file', async () => {
        const result = slices.find((slice) => {
            if (slice == null) return false;
            return slice.path === path.join(testDataDir, 'single/single.json');
        });
        expect(result).toMatchObject({
            length: 364
        });
    });

    it('should properly slice the array json file', async () => {
        const result = slices.find((slice) => {
            if (slice == null) return false;
            return slice.path === path.join(testDataDir, 'array/array.json');
        });
        expect(result).toMatchObject({
            length: 1822
        });
    });
});

describe('File slicer non json files', () => {
    let slices: (SliceRequest|null)[];
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
                    line_delimiter: '\n',
                    file_per_slice: false
                },
                {
                    _op: 'noop'
                }
            ]
        });

        harness = new SlicerTestHarness(job, {});

        await harness.initialize();

        slices = await harness.getAllSlices() as (SliceRequest|null)[];
    });

    afterAll(async () => {
        await harness.shutdown();
    });

    it('properly slices non-JSON files', async () => {
        const flatSlices: any[] = flatten(slices as any);

        expect(flatSlices.length).toBe(47);
        expect(flatSlices[22].length).toEqual(321);
        expect(flatSlices[45].length).toEqual(321);
    });
});
