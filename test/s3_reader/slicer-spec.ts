import { newTestJobConfig, SlicerTestHarness } from 'teraslice-test-harness';
import { SlicedFileResults } from '../../asset/src/__lib/interfaces';

describe('S3 slicer when slicing JSON objects', () => {
    let s3ParamsFirstRequest: any;
    let s3ParamsSecondRequest: any;

    const response = {
        IsTruncated: true,
        Contents: [
            {
                Key: 'testing/obj0',
                Size: 1000
            },
            {
                Key: 'testing/obj1',
                Size: 500
            },
            {
                Key: 'testing/obj2',
                Size: 500
            }
        ]
    };

    const clients = [
        {
            type: 's3',
            create: () => ({
                client: {
                    firstRun: true,
                    listObjects_Async(params: any) {
                        if (this.firstRun) {
                            // extract the s3 request options for validation
                            s3ParamsFirstRequest = params;
                            this.firstRun = false;
                            return response;
                        }
                        response.IsTruncated = false;
                        s3ParamsSecondRequest = params;
                        return response;
                    },
                },
            }),
        },
    ];

    const job = newTestJobConfig({
        analytics: true,
        operations: [
            {
                _op: 's3_reader',
                path: 'data-store/testing/',
                connection: 'default',
                size: 500,
                format: 'json'
            },
            {
                _op: 'noop'
            }
        ]
    });

    let harness: SlicerTestHarness;

    beforeEach(async () => {
        harness = new SlicerTestHarness(job, {
            clients,
        });

        await harness.initialize();
    });

    afterEach(async () => {
        await harness.shutdown();
    });

    it('should generate whole-object slices.', async () => {
        // Expecting a truncated response, so we need to call slice() twice
        const firstBatch = await harness.createSlices();
        const secondBatch = await harness.createSlices();
        const slices = firstBatch.concat(secondBatch);

        expect(slices.length).toBe(6);

        // Verify the S3 request parameters are accurate
        expect(s3ParamsFirstRequest.Bucket).toEqual('data-store');
        expect(s3ParamsFirstRequest.Prefix).toEqual('testing/');
        expect(s3ParamsFirstRequest.Marker).toEqual(undefined);

        expect(s3ParamsSecondRequest.Bucket).toEqual('data-store');
        expect(s3ParamsSecondRequest.Prefix).toEqual('testing/');
        expect(s3ParamsSecondRequest.Marker).toEqual('testing/obj2');

        slices.forEach((record) => {
            expect(record.offset).toEqual(0);
            if (record.path === 'testing/obj0') {
                expect(record.length).toEqual(1000);
            } else {
                expect(record.length).toEqual(500);
            }
        });
    });
});

describe('S3 slicer when slicing other objects', () => {
    const clients = [
        {
            type: 's3',
            create: () => ({
                client: {
                    firstRun: true,
                    listObjects_Async() {
                        return {
                            Contents: [
                                {
                                    Key: 'testing/obj0',
                                    Size: 1000
                                },
                                {
                                    Key: 'testing/obj1',
                                    Size: 500
                                },
                                {
                                    Key: 'testing/obj2',
                                    Size: 500
                                }
                            ]
                        };
                    },
                },
            }),
        },
    ];

    const job = newTestJobConfig({
        analytics: true,
        operations: [
            {
                _op: 's3_reader',
                path: 'data-store',
                connection: 'default',
                object_prefix: 'testing/',
                size: 500,
                format: 'ldjson'
            },
            {
                _op: 'noop'
            }
        ]
    });

    let harness: SlicerTestHarness;

    beforeEach(async () => {
        harness = new SlicerTestHarness(job, {
            clients,
        });

        await harness.initialize();
    });

    afterEach(async () => {
        await harness.shutdown();
    });

    it('should generate regular slices.', async () => {
        const slices = await harness.createSlices();
        expect(slices.length).toBe(4);

        const record1 = slices.pop() as SlicedFileResults;

        expect(record1).toBeDefined();
        expect(record1.offset).toEqual(0);
        expect(record1.length).toEqual(500);

        const record2 = slices.pop() as SlicedFileResults;

        expect(record2).toBeDefined();
        expect(record2.offset).toEqual(0);
        expect(record2.length).toEqual(500);

        const record3 = slices.pop() as SlicedFileResults;

        expect(record3).toBeDefined();
        // Middle slices have an extra byte at the beginning for line delimiter detection
        expect(record3.offset).toEqual(499);
        expect(record3.length).toEqual(501);

        const record4 = slices.pop() as SlicedFileResults;

        expect(record4).toBeDefined();
        expect(record4.offset).toEqual(0);
        expect(record4.length).toEqual(500);

        const [allDone] = await harness.createSlices();

        expect(allDone).toBeNull();
    });
});
