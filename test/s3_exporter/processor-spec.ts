import 'jest-extended';
import { WorkerTestHarness } from 'teraslice-test-harness';
import {
    DataEntity, debugLogger, toString, get
} from '@terascope/core-utils';
import {
    Format, Compressor, getS3Object,
    S3Client,
    Compression
} from '@terascope/file-asset-apis';
import { OpConfig, TestClientConfig } from '@terascope/job-components';
import { makeClient, cleanupBucket, getBodyFromResults } from '../helpers/index.js';
import { DEFAULT_API_NAME, S3ExporterAPIConfig } from '../../asset/src/s3_sender_api/interfaces.js';

describe('S3 sender api', () => {
    const bucket = 's3-exporter';
    const dirPath = '/testing/';
    const path = `${bucket}${dirPath}`;
    const logger = debugLogger('test');

    let compressor: Compressor;
    let harness: WorkerTestHarness;
    let workerId: string;
    let data: DataEntity[];
    let client: S3Client;
    let clients: TestClientConfig[];

    async function makeTest(config?: {
        _op: Partial<OpConfig>;
        api: Partial<S3ExporterAPIConfig>;
    }) {
        const _op = {
            _op: 's3_exporter',
            _api_name: DEFAULT_API_NAME
        };

        const api: S3ExporterAPIConfig = {
            id: 'test',
            _name: DEFAULT_API_NAME,
            path,
            _connection: 'my-s3-connector',
            file_per_slice: true,
            compression: Compression.none,
            format: Format.csv,
            field_delimiter: ',',
            line_delimiter: '\n',
            include_header: false
        };

        const opConfig = config?._op ? Object.assign({}, _op, config._op) : _op;
        const apiConfig = config?.api ? Object.assign({}, api, config.api) : api;
        harness = WorkerTestHarness.testSender(opConfig, apiConfig, { clients });

        compressor = new Compressor(apiConfig.compression);

        await harness.initialize();
        workerId = toString(get(harness, 'context.cluster.worker.id'));

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
                    };
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
        const test = await makeTest({ _op: {}, api: { format } });

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
