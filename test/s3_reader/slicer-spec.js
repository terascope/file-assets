'use strict';

// const { TestContext } = require('@terascope/job-components');
const {
    newTestJobConfig,
    SlicerTestHarness,
} = require('teraslice-test-harness');

describe('S3 slicer when slicing JSON objects', () => {
    let s3ParamsFirstRequest;
    let s3ParamsSecondRequest;

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
                    listObjects_Async(params) {
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
                bucket: 'data-store',
                connection: 'default',
                object_prefix: 'testing/',
                size: 500,
                format: 'json'
            },
            {
                _op: 'noop'
            }
        ]
    });

    let harness;

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
                bucket: 'data-store',
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

    let harness;

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

        let currentRecord = slices.pop();
        expect(currentRecord.offset).toEqual(0);
        expect(currentRecord.length).toEqual(500);
        currentRecord = slices.pop();
        expect(currentRecord.offset).toEqual(0);
        expect(currentRecord.length).toEqual(500);
        currentRecord = slices.pop();
        // Middle slices have an extra byte at the beginning for line delimiter detection
        expect(currentRecord.offset).toEqual(499);
        expect(currentRecord.length).toEqual(501);
        currentRecord = slices.pop();
        expect(currentRecord.offset).toEqual(0);
        expect(currentRecord.length).toEqual(500);
    });
});
