import 'jest-extended';
import { WorkerTestHarness, newTestJobConfig } from 'teraslice-test-harness';

describe('S3 reader\'s fetcher', () => {
    let harness: WorkerTestHarness;
    let s3Params: any = {};

    const response = {
        Body: 'this\tis\tsome\tcsv\ttest\tdata\n'
    };

    const mockClient = {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        getObject_Async: (params: any) => {
            s3Params = params;
            return Promise.resolve(response);
        }
    };

    const clients = [
        {
            type: 's3',
            endpoint: 'my-s3-connector',
            create: () => ({
                client: mockClient
            }),
        },
    ];

    beforeEach(async () => {
        const job = newTestJobConfig({
            analytics: true,
            operations: [
                {
                    _op: 's3_reader',
                    path: 'data-store/my/test/',
                    connection: 'my-s3-connector',
                    size: 27,
                    format: 'tsv',
                    field_delimiter: ',',
                    line_delimiter: '\n',
                    compression: 'none'
                },
                {
                    _op: 'noop'
                }
            ]
        });

        harness = new WorkerTestHarness(job, {
            clients,
        });

        await harness.initialize();
    });

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    const slice = {
        path: 'my/test/data.csv',
        offset: 0,
        length: 27
    };

    it('generated the proper S3 getObject settings', async () => {
        const data = await harness.runSlice(slice);

        expect(s3Params.Range).toEqual('bytes=0-26');
        expect(Object.keys(data[0])).toBeArrayOfSize(6);
    });
});
