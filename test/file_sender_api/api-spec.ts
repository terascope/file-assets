import 'jest-extended';
import { WorkerTestHarness } from 'teraslice-test-harness';
import { AnyObject, DataEntity } from '@terascope/job-components';
import { Format } from '@terascope/file-asset-apis';
import fs from 'node:fs';
import path from 'node:path';
import { FileSenderFactoryAPI } from '../../asset/src/file_sender_api/interfaces.js';
// @ts-expect-error
import fixtures from 'jest-fixtures';


describe('File Sender API', () => {
    let harness: WorkerTestHarness;
    let data: DataEntity[];
    let workerId: number;

    async function makeTest(config: AnyObject = {}) {
        const opConfig = {
            _op: 'file_exporter',
            format: Format.ldjson,
            ...config
        };

        harness = WorkerTestHarness.testProcessor(opConfig);

        await harness.initialize();
        // @ts-expect-error
        workerId = harness.context.cluster.worker.id;

        return harness.getAPI<FileSenderFactoryAPI>('file_sender_api:file_exporter-1');
    }

    beforeEach(() => {
        data = [
            DataEntity.make(
                {
                    field1: 42,
                    field3: 'test data',
                    field2: 55
                }
            ),
            DataEntity.make({
                field1: 43,
                field3: 'more test data',
                field2: 56
            }),
            DataEntity.make({
                field1: 44,
                field3: 'even more test data',
                field2: 57
            })
        ];
    });

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    it('can instantiate', async () => {
        const testDataDir = await fixtures.createTempDir();
        const senderApi = await makeTest({ path: testDataDir });
        // file_reader makes one on initialization
        expect(senderApi.size).toEqual(1);
    });

    it('can make a sender', async () => {
        const testDataDir = await fixtures.createTempDir();
        const otherPath = 'other_path';

        expect(fs.readdirSync(testDataDir).length).toEqual(0);

        const senderApi = await makeTest({ path: testDataDir });
        // workerId is made after making test
        const expectedFileName = `${workerId}.ldjson`;

        const sender = await senderApi.create('test', {});

        await sender.verify(otherPath);

        expect(fs.readdirSync(testDataDir)).toContain(otherPath);

        await sender.send(data);

        expect(fs.readdirSync(testDataDir)).toContain(expectedFileName);

        const filePath = path.join(testDataDir, expectedFileName);

        expect(fs.readFileSync(filePath, 'utf-8')).toEqual(
            '{"field1":42,"field3":"test data","field2":55}\n'
            + '{"field1":43,"field3":"more test data","field2":56}\n'
            + '{"field1":44,"field3":"even more test data","field2":57}\n'
        );
    });
});
