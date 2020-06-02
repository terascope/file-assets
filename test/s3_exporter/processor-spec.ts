import { WorkerTestHarness } from 'teraslice-test-harness';
import { TestClientConfig, DataEntity } from '@terascope/job-components';
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import lz4 from 'lz4';
import { ungzip } from 'node-gzip';
import { S3PutConfig } from '../../asset/src/s3_exporter/interfaces';

describe('S3 exporter processor', () => {
    let harness: WorkerTestHarness;
    let workerId: string;
    let data: DataEntity[];
    let routeSlice: DataEntity[];
    const metaRoute1 = '0';
    const metaRoute2 = '1';
    // eslint-disable-next-line prefer-const
    let bucketExists = true;
    let createBucketCalled = false;
    let s3PutParams: S3PutConfig[] = [];

    const s3Client: TestClientConfig = {
        type: 's3',
        endpoint: 'my-s3-connector',
        create: () => ({
            client: {
                // eslint-disable-next-line @typescript-eslint/naming-convention
                putObject_Async: (putParams: S3PutConfig) => {
                    s3PutParams.push(putParams);
                    return Promise.resolve();
                },
                // eslint-disable-next-line @typescript-eslint/naming-convention
                headBucket_Async: (_params: { Bucket: string }) => {
                    if (!bucketExists) throw new Error('I exists');
                    return Promise.resolve();
                },
                // eslint-disable-next-line @typescript-eslint/naming-convention
                createBucket_Async: (_params: { Bucket: string }) => {
                    createBucketCalled = true;
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
        // @ts-expect-error
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

        routeSlice = [
            DataEntity.make(
                {
                    field1: 'first',

                },
                { 'standard:route': metaRoute1 }
            ),
            DataEntity.make(
                {
                    field1: 'second',
                },
                { 'standard:route': metaRoute2 }
            )
        ];
    });

    afterEach(async () => {
        s3PutParams = [];
        createBucketCalled = false;
        bucketExists = true;
        if (harness) await harness.shutdown();
    });

    it('if bucket does not exists, it will create one', async () => {
        const config = { format: 'json' };
        bucketExists = false;
        const test = await makeTest(config);

        await test.runSlice(routeSlice);

        expect(createBucketCalled).toBeTruthy();
    });

    it('if bucket exists, create will not be called', async () => {
        const config = { format: 'json' };
        const test = await makeTest(config);

        await test.runSlice(routeSlice);

        expect(createBucketCalled).toBeFalsy();
    });

    it('generates a csv object', async () => {
        const test = await makeTest();

        await test.runSlice(data);

        const results = s3PutParams.shift() as S3PutConfig;

        expect(results.Body).toEqual('0,1,2,3,4\n');
        expect(results.Key).toEqual(`testing/${workerId}.0`);
        expect(results.Bucket).toEqual('data-store');
    });

    it('generates a tsv object', async () => {
        const config = { format: 'tsv' };
        const test = await makeTest(config);

        await test.runSlice(data);

        const results = s3PutParams.shift() as S3PutConfig;

        expect(results.Body).toEqual('0\t1\t2\t3\t4\n');
        expect(results.Key).toEqual(`testing/${workerId}.0`);
        expect(results.Bucket).toEqual('data-store');
    });

    it('generates a tsv/csv object with an empty slice', async () => {
        const test = await makeTest();

        await test.runSlice(emptySlice);

        const results = s3PutParams.shift() as S3PutConfig;

        expect(results).toBeUndefined();
    });

    it('generates a raw object', async () => {
        const config = { format: 'raw' };
        const test = await makeTest(config);

        await test.runSlice(rawSlice);

        const results = s3PutParams.shift() as S3PutConfig;

        expect(results.Body).toEqual('This is a sentence.\n');
        // The previous test will still increment the slice count
        expect(results.Key).toEqual(`testing/${workerId}.0`);
    });

    it('generates an ldjson object and exludes a field', async () => {
        const config = { format: 'ldjson' };
        const test = await makeTest(config);

        await test.runSlice(data);

        const results = s3PutParams.shift() as S3PutConfig;

        expect(results.Body).toEqual('{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4}\n');
        expect(results.Key).toEqual(`testing/${workerId}.0`);
    });

    it('generates an ldjson object', async () => {
        const config = { format: 'ldjson', fields: [] };
        const test = await makeTest(config);

        await test.runSlice(data);

        const results = s3PutParams.shift() as S3PutConfig;

        expect(results.Body).toEqual('{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4,"field5":5}\n');
        expect(results.Key).toEqual(`testing/${workerId}.0`);
    });

    it('generates a json object', async () => {
        const config = { format: 'json' };
        const test = await makeTest(config);

        await test.runSlice(data);

        const results = s3PutParams.shift() as S3PutConfig;

        expect(results.Body).toEqual('[{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4,"field5":5}]\n');
        expect(results.Key).toEqual(`testing/${workerId}.0`);
    });

    it('generates lz4 compressed object', async () => {
        const config = { format: 'json', compression: 'lz4' };
        const test = await makeTest(config);

        await test.runSlice(data);

        const results = s3PutParams.shift() as S3PutConfig;

        expect(lz4.decode(Buffer.from(results.Body)).toString()).toEqual('[{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4,"field5":5}]\n');
        // No file extensions since not configures in opConfig
        expect(results.Key).toEqual(`testing/${workerId}.0`);
    });

    it('generates gzip compressed object', async () => {
        const config = { format: 'json', compression: 'gzip' };
        const test = await makeTest(config);

        await test.runSlice(data);

        const results = s3PutParams.shift() as S3PutConfig;

        const decompressedObj = await ungzip(Buffer.from(results.Body));
        const expectedBody = decompressedObj.toString();

        expect(expectedBody).toEqual('[{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4,"field5":5}]\n');
        // No file extensions since not configures in opConfig
        expect(results.Key).toEqual(`testing/${workerId}.0`);
    });

    it('can respect metadata routing', async () => {
        const config = { format: 'json' };
        const test = await makeTest(config);

        await test.runSlice(routeSlice);

        const results1 = s3PutParams.shift() as S3PutConfig;

        const expectedBody = results1.Body;

        expect(expectedBody).toEqual('[{"field1":"first"}]\n');
        // No file extensions since not configures in opConfig
        expect(results1.Key).toEqual(`testing/0/${workerId}.0`);

        const results2 = s3PutParams.shift() as S3PutConfig;

        const expectedBody2 = results2.Body;

        expect(expectedBody2).toEqual('[{"field1":"second"}]\n');
        // No file extensions since not configures in opConfig
        expect(results2.Key).toEqual(`testing/1/${workerId}.0`);
    });
});
