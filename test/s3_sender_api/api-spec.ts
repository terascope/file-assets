import 'jest-extended';
import { WorkerTestHarness } from 'teraslice-test-harness';
import {
    DataEntity, debugLogger, get, toString
} from '@terascope/core-utils';
import { TestClientConfig } from '@terascope/job-components';
// @ts-expect-error
import lz4init from 'lz4-asm/dist/lz4asm';
import pkg from 'node-gzip';
import {
    Format, Compression, Compressor,
    listS3Buckets, getS3Object, S3Client
} from '@terascope/file-asset-apis';
import { makeClient, cleanupBucket, getBodyFromResults } from '../helpers/index.js';
import { DEFAULT_API_NAME, S3SenderFactoryAPI } from '../../asset/src/s3_sender_api/interfaces.js';

const { ungzip } = pkg;
const lz4Module = {};
const lz4Ready = lz4init(lz4Module);

describe('S3 sender api', () => {
    const bucket = 's3-api-sender';
    const dirPath = '/testing/';
    const path = `${bucket}${dirPath}`;
    const logger = debugLogger('test');

    let compressor: Compressor;
    let harness: WorkerTestHarness;
    let workerId: string;
    let data: DataEntity[];
    let routeSlice: DataEntity[];
    const metaRoute1 = '0';
    const metaRoute2 = '1';

    let client: S3Client;
    let clients: TestClientConfig[];

    beforeAll(async () => {
        client = await makeClient();
        clients = [
            {
                type: 's3',
                endpoint: 'my-s3-connector',
                async createClient() {
                    return {
                        client,
                        logger
                    };
                },
            },
        ];
    });

    const rawSlice = [DataEntity.make({ data: 'This is a sentence.' })];

    // FIXME the config type should not be so loose
    async function makeAPITest(config?: Record<string, any>) {
        const api = {
            _name: DEFAULT_API_NAME,
            path,
            _connection: 'my-s3-connector',
            file_per_slice: true,
            compression: Compression.none,
            format: Format.csv,
            include_header: false
        };
        const apiConfig = config ? Object.assign({}, api, config) : api;
        harness = WorkerTestHarness.testSender({ _op: 's3_exporter', _api_name: DEFAULT_API_NAME }, apiConfig, { clients });

        compressor = new Compressor(apiConfig.compression);

        await harness.initialize();
        workerId = toString(get(harness, 'context.cluster.worker.id'));

        return harness.getAPI<S3SenderFactoryAPI>(DEFAULT_API_NAME);
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

    async function getBucketList(): Promise<string[]> {
        const response = await listS3Buckets(client);
        if (!response.Buckets) return [];
        return response.Buckets.filter((bucketObj) => bucketObj.Name === bucket) as string[];
    }

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    // NOTE: this test needs to be first
    it('if bucket does not exists, it will create one', async () => {
        const config = { format: 'json' };

        const listResponseBefore = await getBucketList();
        expect(listResponseBefore).toBeArrayOfSize(0);

        const apiManager = await makeAPITest(config);
        await apiManager.create('test1', {});

        const listResponseAfter = await getBucketList();
        expect(listResponseAfter).toBeArrayOfSize(1);
    });

    it('generates a csv object', async () => {
        const expectedResults = '0,1,2,3,4,5\n';
        const format = Format.csv;
        const apiManager = await makeAPITest({ format });
        const api = await apiManager.create(format, {});

        await api.send(data);

        const key = `testing/${workerId}.0.${format}`;

        const dbData = await getS3Object(client, {
            Bucket: bucket,
            Key: key,
        });

        const body = await getBodyFromResults(dbData);

        const fetchedData = await compressor.decompress(
            body
        );
        expect(fetchedData).toEqual(expectedResults);
    });

    it('generates a tsv object', async () => {
        const expectedResults = '0\t1\t2\t3\t4\t5\n';
        const format = Format.tsv;
        const apiManager = await makeAPITest({ format });
        const api = await apiManager.create(format, {});

        await api.send(data);

        const key = `testing/${workerId}.0.${format}`;

        const dbData = await getS3Object(client, {
            Bucket: bucket,
            Key: key,
        });

        const body = await getBodyFromResults(dbData);

        const fetchedData = await compressor.decompress(
            body
        );
        expect(fetchedData).toEqual(expectedResults);
    });

    it('generates a raw object', async () => {
        const expectedResults = `${rawSlice[0].data}\n`;
        const format = Format.raw;
        const apiManager = await makeAPITest({ format });
        const api = await apiManager.create(format, {});

        await api.send(rawSlice);

        const key = `testing/${workerId}.0`;

        const dbData = await getS3Object(client, {
            Bucket: bucket,
            Key: key,
        });

        const body = await getBodyFromResults(dbData);

        const fetchedData = await compressor.decompress(
            body
        );
        expect(fetchedData).toEqual(expectedResults);
    });

    it('generates an ldjson object', async () => {
        const expectedResults = '{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4,"field5":5}\n';
        const format = Format.ldjson;
        const apiManager = await makeAPITest({ format });
        const api = await apiManager.create(format, {});

        await api.send(data);

        const key = `testing/${workerId}.0.${format}`;

        const dbData = await getS3Object(client, {
            Bucket: bucket,
            Key: key,
        });

        const body = await getBodyFromResults(dbData);

        const fetchedData = await compressor.decompress(
            body
        );
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
        const apiManager = await makeAPITest({ format, fields });
        const api = await apiManager.create(format, {});

        await api.send(data);

        const key = `testing/${workerId}.0.${format}`;

        const dbData = await getS3Object(client, {
            Bucket: bucket,
            Key: key,
        });

        const body = await getBodyFromResults(dbData);

        const fetchedData = await compressor.decompress(
            body
        );
        expect(fetchedData).toEqual(expectedResults);
    });

    it('generates a json object', async () => {
        const expectedResults = '[{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4,"field5":5}]\n';
        const format = Format.json;
        const apiManager = await makeAPITest({ format });
        const api = await apiManager.create(format, {});

        await api.send(data);

        const key = `testing/${workerId}.0.${format}`;

        const dbData = await getS3Object(client, {
            Bucket: bucket,
            Key: key,
        });

        const body = await getBodyFromResults(dbData);

        const fetchedData = await compressor.decompress(
            body
        );
        expect(fetchedData).toEqual(expectedResults);
    });

    it('generates lz4 compressed object', async () => {
        const compression = Compression.lz4;
        const format = Format.json;
        const apiManager = await makeAPITest({ format, compression });
        const api = await apiManager.create(format, {});

        await api.send(data);

        const key = `testing/${workerId}.0.${format}.lz4`;

        const dbData = await getS3Object(client, {
            Bucket: bucket,
            Key: key,
        });

        const buf = await getBodyFromResults(dbData);
        const fetchedData = await compressor.decompress(
            buf
        );
        const { lz4js } = await lz4Ready;
        expect(fetchedData).toEqual(
            lz4js.decompress(buf).toString()
        );
    });

    it('generates gzip compressed object', async () => {
        const compression = Compression.gzip;
        const format = Format.json;
        const apiManager = await makeAPITest({ format, compression });
        const api = await apiManager.create(format, {});

        await api.send(data);

        const key = `testing/${workerId}.0.${format}.gz`;

        const dbData = await getS3Object(client, {
            Bucket: bucket,
            Key: key,
        });

        // @ts-expect-error types are not correct from aws
        const rawBody = await dbData.Body.transformToByteArray();
        const fetchedData = await compressor.decompress(
            Buffer.from(rawBody)
        );
        expect(fetchedData).toEqual(
            (await ungzip(Buffer.from(rawBody))).toString()
        );
    });

    it('does not respect routing unless a router is being used', async () => {
        const expectedResults = '[{"field1":"first"},{"field1":"second"}]\n';
        const format = Format.json;
        const apiManager = await makeAPITest({ format });
        const api = await apiManager.create(format, {});

        await api.send(routeSlice);

        const key = `testing/${workerId}.0.${format}`;

        const dbData = await getS3Object(client, {
            Bucket: bucket,
            Key: key,
        });

        const body = await getBodyFromResults(dbData);

        const fetchedData = await compressor.decompress(
            body
        );
        expect(fetchedData).toEqual(expectedResults);
    });

    it('can respect metadata routing is used by a router with deprecated settings', async () => {
        const expectedResults1 = '[{"field1":"first"}]\n';
        const expectedResults2 = '[{"field1":"second"}]\n';

        const format = Format.json;
        const apiManager = await makeAPITest({ format, _key: 'a' });
        const api = await apiManager.create(format, {});

        await api.send(routeSlice);

        const key1 = `testing/${metaRoute1}/${workerId}.0.json`;
        const key2 = `testing/${metaRoute2}/${workerId}.0.json`;

        const dbData1 = await getS3Object(client, {
            Bucket: bucket,
            Key: key1,
        });

        const dbData2 = await getS3Object(client, {
            Bucket: bucket,
            Key: key2,
        });

        const body1 = await getBodyFromResults(dbData1);
        const body2 = await getBodyFromResults(dbData2);

        const fetchedData1 = await compressor.decompress(
            body1
        );
        expect(fetchedData1).toEqual(expectedResults1);

        const fetchedData2 = await compressor.decompress(
            body2
        );
        expect(fetchedData2).toEqual(expectedResults2);
    });

    it('can respect metadata routing is used by a router with correct settings', async () => {
        const expectedResults1 = '[{"field1":"first"}]\n';
        const expectedResults2 = '[{"field1":"second"}]\n';

        const format = Format.json;
        const apiManager = await makeAPITest({ format, dynamic_routing: true });
        const api = await apiManager.create(format, {});

        await api.send(routeSlice);

        const key1 = `testing/${metaRoute1}/${workerId}.0.json`;
        const key2 = `testing/${metaRoute2}/${workerId}.0.json`;

        const dbData1 = await getS3Object(client, {
            Bucket: bucket,
            Key: key1,
        });

        const dbData2 = await getS3Object(client, {
            Bucket: bucket,
            Key: key2,
        });

        const body1 = await getBodyFromResults(dbData1);
        const body2 = await getBodyFromResults(dbData2);

        const fetchedData1 = await compressor.decompress(
            body1
        );
        expect(fetchedData1).toEqual(expectedResults1);

        const fetchedData2 = await compressor.decompress(
            body2
        );
        expect(fetchedData2).toEqual(expectedResults2);
    });
});
