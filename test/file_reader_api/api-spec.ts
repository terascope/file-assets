import 'jest-extended';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';
import { AnyObject } from '@terascope/job-components';
import { Format } from '@terascope/file-asset-apis';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { FileReaderFactoryAPI } from '../../asset/src/file_reader_api/interfaces.js';
// @ts-expect-error
import fixtures from 'jest-fixtures';

const dirname = path.dirname(fileURLToPath(import.meta.url));

describe('File Reader API', () => {
    let harness: WorkerTestHarness;
    let testDataDir: string;

    beforeAll(async () => {
        testDataDir = await fixtures.copyFixtureIntoTempDir(dirname, 'file_reader/ldjson/subdir');
    });

    async function makeTest(config: AnyObject = {}) {
        const opConfig = {
            _op: 'file_reader',
            path: testDataDir,
            format: Format.ldjson,
            ...config
        };
        const job = newTestJobConfig({
            operations: [
                opConfig,
                {
                    _op: 'noop'
                }
            ]
        });

        harness = new WorkerTestHarness(job);

        await harness.initialize();

        return harness.getAPI<FileReaderFactoryAPI>('file_reader_api:file_reader-0');
    }

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    it('can instantiate', async () => {
        const readerApi = await makeTest();
        // file_reader makes one on initialization
        expect(readerApi.size).toEqual(1);
    });

    it('can make a reader', async () => {
        const slice = {
            path: path.join(testDataDir, 'testData.txt'),
            offset: 0,
            length: 750,
            total: 16820
        };

        const readerApi = await makeTest();

        const reader = await readerApi.create('test', {} as any);
        expect(reader.read).toBeFunction();

        const results = await reader.read(slice);

        expect(results).toBeArrayOfSize(3);
    });
});
