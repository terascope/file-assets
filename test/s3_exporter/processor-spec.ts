import 'jest-extended';
import { WorkerTestHarness } from 'teraslice-test-harness';
import { DataEntity, TestClientConfig, debugLogger } from '@terascope/job-components';
import {
    Format, Compressor, getS3Object,
    S3Client
} from '@terascope/file-asset-apis';
import { makeClient, cleanupBucket, getBodyFromResults } from '../helpers/index.js';

describe('S3 sender api', () => {
    const bucket = 's3-exporter';
    const dirPath = '/testing/';
    const path = `${bucket}${dirPath}`;
    const logger = debugLogger('test');

    let compressor: Compressor;
    let harness: WorkerTestHarness;
    let workerId: number;
    let data: DataEntity[];
    let client: S3Client;
    let clients: TestClientConfig[];

    async function makeTest(config?: any) {
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

        compressor = new Compressor(opConfig.compression);

        await harness.initialize();
        // @ts-expect-error
        workerId = harness.context.cluster.worker.id;

        return harness;
    }

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
                    }
                },
            },
        ];
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
    });

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    it('can send data', async () => {
        const expectedResults = '0,1,2,3,4,5\n';
        const format = Format.csv;
        const test = await makeTest({ format });

        const results = await test.runSlice(data);

        expect(results).toBeArrayOfSize(1);
        expect(results[0]).toMatchObject(data[0]);

        const key = `testing/${workerId}.0.csv`;

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
});
