import 'jest-extended';
import path from 'node:path';
import { flatten } from '@terascope/core-utils';
import { Format } from '@terascope/file-asset-apis';
import { SliceRequest } from '@terascope/job-components';
import { newTestJobConfig, SlicerTestHarness } from 'teraslice-test-harness';
import { fileURLToPath } from 'node:url';
// @ts-expect-error
import fixtures from 'jest-fixtures';
import { DEFAULT_API_NAME } from '../../asset/src/file_reader_api/interfaces.js';

const dirname = path.dirname(fileURLToPath(import.meta.url));

describe('File slicer json files', () => {
    let harness: SlicerTestHarness;
    let testDataDir: string;
    let slices: (SliceRequest | null)[];

    beforeAll(async () => {
        testDataDir = await fixtures.copyFixtureIntoTempDir(dirname, 'file_reader/json');
        const job = newTestJobConfig({
            analytics: true,
            apis: [
                {
                    _name: DEFAULT_API_NAME,
                    size: 750,
                    path: testDataDir,
                    format: Format.json
                }
            ],
            operations: [
                {
                    _op: 'file_reader',
                    _api_name: DEFAULT_API_NAME,
                },
                {
                    _op: 'noop'
                }
            ]
        });

        harness = new SlicerTestHarness(job, {});

        await harness.initialize();

        slices = await harness.getAllSlices() as (SliceRequest | null)[];
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
            length: 365
        });
    });

    it('should properly slice the array json file', async () => {
        const result = slices.find((slice) => {
            if (slice == null) return false;
            return slice.path === path.join(testDataDir, 'array/array.json');
        });
        expect(result).toMatchObject({
            length: 1827
        });
    });
});

describe('File slicer non json files', () => {
    let slices: (SliceRequest | null)[];
    let harness: SlicerTestHarness;
    let testDataDir: string;

    beforeEach(async () => {
        testDataDir = await fixtures.copyFixtureIntoTempDir(dirname, 'file_reader/ldjson');
        const job = newTestJobConfig({
            analytics: true,
            apis: [
                {
                    _name: DEFAULT_API_NAME,
                    path: testDataDir,
                    format: Format.ldjson,
                    size: 750,
                    line_delimiter: '\n',
                    file_per_slice: false
                }
            ],
            operations: [
                {
                    _op: 'file_reader',
                    _api_name: DEFAULT_API_NAME,
                },
                {
                    _op: 'noop'
                }
            ]
        });

        harness = new SlicerTestHarness(job, {});

        await harness.initialize();

        slices = await harness.getAllSlices() as (SliceRequest | null)[];
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
