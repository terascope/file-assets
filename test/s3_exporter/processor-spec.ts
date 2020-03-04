import { WorkerTestHarness } from 'teraslice-test-harness';
import { TestClientConfig } from '@terascope/job-components';
import { DataEntity } from '@terascope/utils';
// @ts-ignore
import lz4 from 'lz4';
import { ungzip } from 'node-gzip';

describe('S3 exporter processor', () => {
    let harness: WorkerTestHarness;
    let workerId: string;
    let data: DataEntity[];

    let s3PutParams: any;

    const s3Client: TestClientConfig = {
        type: 's3',
        endpoint: 'my-s3-connector',
        create: () => ({
            client: {
                putObject_Async: (putParams: any) => {
                    s3PutParams = putParams;
                    return Promise.resolve();
                }
            }
        })
    };

    const emptySlice: DataEntity[] = [];
    const rawSlice = [DataEntity.make({ data: 'This is a sentence.' })];

    async function makeTest(config?: any) {
        const _op = {
            _op: 's3_exporter',
            path: '/data-store/testing/',
            connection: 'my-s3-connector',
            file_per_slice: true,
            compression: 'none',
            format: 'csv',
            field_delimiter: ',',
            line_delimiter: '\n',
            // Exclude field5 for testing with the ldjson field filtering
            fields: [
                'field0',
                'field1',
                'field2',
                'field3',
                'field4'
            ],
            include_header: false
        };
        const opConfig = config ? Object.assign({}, _op, config) : _op;
        harness = WorkerTestHarness.testProcessor(opConfig, { clients: [s3Client] });

        await harness.initialize();
        // @ts-ignore
        workerId = harness.context.cluster.worker.id;

        return harness;
    }

    beforeEach(async () => {
        data = [DataEntity.make({
            field0: 0,
            field1: 1,
            field2: 2,
            field3: 3,
            field4: 4,
            field5: 5
        })];
        s3PutParams = undefined;
    });

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    it('generates a csv object', async () => {
        const test = await makeTest();

        await test.runSlice(data);

        expect(s3PutParams.Body).toEqual('0,1,2,3,4\n');
        expect(s3PutParams.Key).toEqual(`testing/${workerId}.0`);
        expect(s3PutParams.Bucket).toEqual('data-store');
    });

    it('generates a tsv object', async () => {
        const config = { format: 'tsv' };
        const test = await makeTest(config);

        await test.runSlice(data);

        expect(s3PutParams.Body).toEqual('0\t1\t2\t3\t4\n');
        expect(s3PutParams.Key).toEqual(`testing/${workerId}.0`);
        expect(s3PutParams.Bucket).toEqual('data-store');
    });

    it('generates a tsv/csv object with an empty slice', async () => {
        const test = await makeTest();

        await test.runSlice(emptySlice);

        expect(s3PutParams).toBeUndefined();
    });

    it('generates a raw object', async () => {
        const config = { format: 'raw' };
        const test = await makeTest(config);

        await test.runSlice(rawSlice);

        expect(s3PutParams.Body).toEqual('This is a sentence.\n');
        // The previous test will still increment the slice count
        expect(s3PutParams.Key).toEqual(`testing/${workerId}.0`);
    });

    it('generates an ldjson object and exludes a field', async () => {
        const config = { format: 'ldjson' };
        const test = await makeTest(config);

        await test.runSlice(data);

        expect(s3PutParams.Body).toEqual('{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4}\n');
        expect(s3PutParams.Key).toEqual(`testing/${workerId}.0`);
    });

    it('generates an ldjson object', async () => {
        const config = { format: 'ldjson', fields: [] };
        const test = await makeTest(config);

        await test.runSlice(data);

        expect(s3PutParams.Body).toEqual('{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4,"field5":5}\n');
        expect(s3PutParams.Key).toEqual(`testing/${workerId}.0`);
    });

    it('generates a json object', async () => {
        const config = { format: 'json' };
        const test = await makeTest(config);

        await test.runSlice(data);

        expect(s3PutParams.Body).toEqual('[{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4,"field5":5}]\n');
        expect(s3PutParams.Key).toEqual(`testing/${workerId}.0`);
    });

    it('generates lz4 compressed object', async () => {
        const config = { format: 'json', compression: 'lz4' };
        const test = await makeTest(config);

        await test.runSlice(data);

        expect(lz4.decode(Buffer.from(s3PutParams.Body)).toString()).toEqual('[{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4,"field5":5}]\n');
        // No file extensions since not configures in opConfig
        expect(s3PutParams.Key).toEqual(`testing/${workerId}.0`);
    });

    it('generates gzip compressed object', async () => {
        const config = { format: 'json', compression: 'gzip' };
        const test = await makeTest(config);

        await test.runSlice(data);

        const decompressedObj = await ungzip(Buffer.from(s3PutParams.Body));
        const results = decompressedObj.toString();

        expect(results).toEqual('[{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4,"field5":5}]\n');
        // No file extensions since not configures in opConfig
        expect(s3PutParams.Key).toEqual(`testing/${workerId}.0`);
    });
});
