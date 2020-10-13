import 'jest-extended';
import { WorkerTestHarness } from 'teraslice-test-harness';
import { DataEntity, AnyObject } from '@terascope/job-components';
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import lz4 from 'lz4';
import { ungzip } from 'node-gzip';
import { makeClient, cleanupBucket } from '../helpers';
import { S3SenderFactoryAPI } from '../../asset/src/s3_sender_api/interfaces';
import { Format, Compression } from '../../asset/src/__lib/interfaces';
import CompressionFormatter from '../../asset/src/__lib/compression';

describe('S3 sender api', () => {
    const bucket = 's3-api-sender';
    const dirPath = '/testing/';
    const path = `${bucket}${dirPath}`;
    let compressor: CompressionFormatter;
    let harness: WorkerTestHarness;
    let workerId: string;
    let data: DataEntity[];
    let routeSlice: DataEntity[];
    const metaRoute1 = '0';
    const metaRoute2 = '1';

    const client = makeClient();

    const clients = [
        {
            type: 's3',
            endpoint: 'my-s3-connector',
            create: () => ({
                client
            }),
        },
    ];

    const rawSlice = [DataEntity.make({ data: 'This is a sentence.' })];

    async function makeApiTest(config?: any) {
        const _op = {
            _op: 's3_exporter',
            path,
            connection: 'my-s3-connector',
            file_per_slice: true,
            compression: 'none',
            format: 'csv',
            field_delimiter: ',',
            line_delimiter: '\n',
            include_header: false
        };
        const opConfig = config ? Object.assign({}, _op, config) : _op;
        harness = WorkerTestHarness.testProcessor(opConfig, { clients });

        compressor = new CompressionFormatter(opConfig.compression);

        await harness.initialize();

        workerId = harness.context.cluster.worker.id;

        return harness.getAPI<S3SenderFactoryAPI>('s3_sender_api:s3_exporter-1');
    }

    beforeAll(async () => {
        await cleanupBucket(client, bucket);
    });

    afterAll(async () => {
        await cleanupBucket(client, bucket);
    });

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

    async function getBucketList(): Promise<AnyObject[]> {
        const response = await client.listBuckets_Async();
        return response.Buckets.filter((bucketObj: any) => bucketObj.Name === bucket);
    }

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    // NOTE: this test needs to be first
    it('if bucket does not exists, it will create one', async () => {
        const config = { format: 'json' };

        const listResponseBefore = await getBucketList();
        expect(listResponseBefore).toBeArrayOfSize(0);

        const apiManager = await makeApiTest(config);
        await apiManager.create('test1', {});

        const listResponseAfter = await getBucketList();
        expect(listResponseAfter).toBeArrayOfSize(1);
    });

    it('generates a csv object', async () => {
        const expectedResults = '0,1,2,3,4,5\n';
        const format = Format.csv;
        const apiManager = await makeApiTest({ format });
        const api = await apiManager.create(format, {});

        const mockedSend = jest.fn(api.client.putObject_Async);
        api.client.putObject_Async = mockedSend;

        await api.send(data);

        await mockedSend.mock.results[0].value;

        const query = mockedSend.mock.calls[0][0];

        const key = `testing/${workerId}.0`;

        expect(query.Body).toEqual(expectedResults);
        expect(query.Key).toEqual(key);
        expect(query.Bucket).toEqual(bucket);

        const dbData = await client.getObject_Async({
            Bucket: bucket,
            Key: key,
        });

        const fetchedData = await compressor.decompress(dbData.Body);
        expect(fetchedData).toEqual(expectedResults);
    });

    it('generates a tsv object', async () => {
        const expectedResults = '0\t1\t2\t3\t4\t5\n';
        const format = Format.tsv;
        const apiManager = await makeApiTest({ format });
        const api = await apiManager.create(format, {});

        const mockedSend = jest.fn(api.client.putObject_Async);
        api.client.putObject_Async = mockedSend;

        await api.send(data);

        await mockedSend.mock.results[0].value;

        const query = mockedSend.mock.calls[0][0];

        const key = `testing/${workerId}.0`;

        expect(query.Body).toEqual(expectedResults);
        expect(query.Key).toEqual(key);
        expect(query.Bucket).toEqual(bucket);

        const dbData = await client.getObject_Async({
            Bucket: bucket,
            Key: key,
        });

        const fetchedData = await compressor.decompress(dbData.Body);
        expect(fetchedData).toEqual(expectedResults);
    });

    it('generates a raw object', async () => {
        const expectedResults = `${rawSlice[0].data}\n`;
        const format = Format.raw;
        const apiManager = await makeApiTest({ format });
        const api = await apiManager.create(format, {});

        const mockedSend = jest.fn(api.client.putObject_Async);
        api.client.putObject_Async = mockedSend;

        await api.send(rawSlice);

        await mockedSend.mock.results[0].value;

        const query = mockedSend.mock.calls[0][0];

        const key = `testing/${workerId}.0`;

        expect(query.Body).toEqual(expectedResults);
        expect(query.Key).toEqual(key);
        expect(query.Bucket).toEqual(bucket);

        const dbData = await client.getObject_Async({
            Bucket: bucket,
            Key: key,
        });

        const fetchedData = await compressor.decompress(dbData.Body);
        expect(fetchedData).toEqual(expectedResults);
    });

    it('generates an ldjson object', async () => {
        const expectedResults = '{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4,"field5":5}\n';
        const format = Format.ldjson;
        const apiManager = await makeApiTest({ format });
        const api = await apiManager.create(format, {});

        const mockedSend = jest.fn(api.client.putObject_Async);
        api.client.putObject_Async = mockedSend;

        await api.send(data);

        await mockedSend.mock.results[0].value;

        const query = mockedSend.mock.calls[0][0];

        const key = `testing/${workerId}.0`;

        expect(query.Body).toEqual(expectedResults);
        expect(query.Key).toEqual(key);
        expect(query.Bucket).toEqual(bucket);

        const dbData = await client.getObject_Async({
            Bucket: bucket,
            Key: key,
        });

        const fetchedData = await compressor.decompress(dbData.Body);
        expect(fetchedData).toEqual(expectedResults);
    });

    it('generates an ldjson object and excludes a field', async () => {
        // Exclude field5 for testing with the ldjson field filtering
        const fields = [
            'field0',
            'field1',
            'field2',
            'field3',
            'field4'
        ];
        const expectedResults = '{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4}\n';
        const format = Format.ldjson;
        const apiManager = await makeApiTest({ format, fields });
        const api = await apiManager.create(format, {});

        const mockedSend = jest.fn(api.client.putObject_Async);
        api.client.putObject_Async = mockedSend;

        await api.send(data);

        await mockedSend.mock.results[0].value;

        const query = mockedSend.mock.calls[0][0];

        const key = `testing/${workerId}.0`;

        expect(query.Body).toEqual(expectedResults);
        expect(query.Key).toEqual(key);
        expect(query.Bucket).toEqual(bucket);

        const dbData = await client.getObject_Async({
            Bucket: bucket,
            Key: key,
        });

        const fetchedData = await compressor.decompress(dbData.Body);
        expect(fetchedData).toEqual(expectedResults);
    });

    it('generates a json object', async () => {
        const expectedResults = '[{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4,"field5":5}]\n';
        const format = Format.json;
        const apiManager = await makeApiTest({ format });
        const api = await apiManager.create(format, {});

        const mockedSend = jest.fn(api.client.putObject_Async);
        api.client.putObject_Async = mockedSend;

        await api.send(data);

        await mockedSend.mock.results[0].value;

        const query = mockedSend.mock.calls[0][0];

        const key = `testing/${workerId}.0`;

        expect(query.Body).toEqual(expectedResults);
        expect(query.Key).toEqual(key);
        expect(query.Bucket).toEqual(bucket);

        const dbData = await client.getObject_Async({
            Bucket: bucket,
            Key: key,
        });

        const fetchedData = await compressor.decompress(dbData.Body);
        expect(fetchedData).toEqual(expectedResults);
    });

    it('generates lz4 compressed object', async () => {
        const compression = Compression.lz4;
        const expectedResults = '[{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4,"field5":5}]\n';
        const format = Format.json;
        const apiManager = await makeApiTest({ format, compression });
        const api = await apiManager.create(format, {});

        const mockedSend = jest.fn(api.client.putObject_Async);
        api.client.putObject_Async = mockedSend;

        await api.send(data);

        await mockedSend.mock.results[0].value;

        const query = mockedSend.mock.calls[0][0];

        const key = `testing/${workerId}.0`;

        const decodedData = lz4.decode(Buffer.from(query.Body)).toString();

        expect(decodedData).toEqual(expectedResults);
        expect(query.Key).toEqual(key);
        expect(query.Bucket).toEqual(bucket);

        const dbData = await client.getObject_Async({
            Bucket: bucket,
            Key: key,
        });

        const fetchedData = await compressor.decompress(dbData.Body);
        expect(fetchedData).toEqual(expectedResults);
    });

    it('generates gzip compressed object', async () => {
        const compression = Compression.gzip;
        const expectedResults = '[{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4,"field5":5}]\n';
        const format = Format.json;
        const apiManager = await makeApiTest({ format, compression });
        const api = await apiManager.create(format, {});

        const mockedSend = jest.fn(api.client.putObject_Async);
        api.client.putObject_Async = mockedSend;

        await api.send(data);

        await mockedSend.mock.results[0].value;

        const query = mockedSend.mock.calls[0][0];

        const key = `testing/${workerId}.0`;

        const decodedData = await ungzip(Buffer.from(query.Body));
        const expectedBody = decodedData.toString();

        expect(expectedBody).toEqual(expectedResults);
        expect(query.Key).toEqual(key);
        expect(query.Bucket).toEqual(bucket);

        const dbData = await client.getObject_Async({
            Bucket: bucket,
            Key: key,
        });

        const fetchedData = await compressor.decompress(dbData.Body);
        expect(fetchedData).toEqual(expectedResults);
    });

    it('does not respect routing unless a router is being used', async () => {
        const expectedResults = '[{"field1":"first"},{"field1":"second"}]\n';
        const format = Format.json;
        const apiManager = await makeApiTest({ format });
        const api = await apiManager.create(format, {});

        const mockedSend = jest.fn(api.client.putObject_Async);
        api.client.putObject_Async = mockedSend;

        await api.send(routeSlice);

        await mockedSend.mock.results[0].value;

        const query = mockedSend.mock.calls[0][0];

        const key = `testing/${workerId}.0`;

        expect(query.Body).toEqual(expectedResults);
        expect(query.Key).toEqual(key);
        expect(query.Bucket).toEqual(bucket);

        const dbData = await client.getObject_Async({
            Bucket: bucket,
            Key: key,
        });

        const fetchedData = await compressor.decompress(dbData.Body);

        expect(fetchedData).toEqual(expectedResults);
    });

    it('can respect metadata routing is used by a router', async () => {
        const expectedResults1 = '[{"field1":"first"}]\n';
        const expectedResults2 = '[{"field1":"second"}]\n';

        const format = Format.json;
        const apiManager = await makeApiTest({ format, _key: 'a' });
        const api = await apiManager.create(format, {});

        const mockedSend = jest.fn(api.client.putObject_Async);
        api.client.putObject_Async = mockedSend;

        await api.send(routeSlice);

        await Promise.all(mockedSend.mock.results.map((obj: AnyObject) => obj.value));

        const query1 = mockedSend.mock.calls[0][0];
        const query2 = mockedSend.mock.calls[1][0];

        const key1 = `testing/${metaRoute1}/${workerId}.0`;
        const key2 = `testing/${metaRoute2}/${workerId}.0`;

        expect(query1.Body).toEqual(expectedResults1);
        expect(query1.Key).toEqual(key1);
        expect(query1.Bucket).toEqual(bucket);

        expect(query2.Body).toEqual(expectedResults2);
        expect(query2.Key).toEqual(key2);
        expect(query2.Bucket).toEqual(bucket);

        const dbData1 = await client.getObject_Async({
            Bucket: bucket,
            Key: key1,
        });

        const dbData2 = await client.getObject_Async({
            Bucket: bucket,
            Key: key2,
        });

        const fetchedData1 = await compressor.decompress(dbData1.Body);
        expect(fetchedData1).toEqual(expectedResults1);

        const fetchedData2 = await compressor.decompress(dbData2.Body);
        expect(fetchedData2).toEqual(expectedResults2);
    });
});
