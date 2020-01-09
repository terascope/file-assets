'use strict';

const { TestContext } = require('@terascope/job-components');
const { DataEntity } = require('@terascope/utils');
const Promise = require('bluebird');
const lz4 = require('lz4');
const { ungzip } = require('node-gzip');
const Processor = require('../../asset/s3_exporter/processor');

describe('S3 exporter processor', () => {
    let s3PutParams = '';
    const context = new TestContext('s3-exporter', {
        clients: [
            {
                type: 's3',
                endpoint: 'my-s3-connector',
                create: () => ({
                    client: {
                        putObject_Async: (putParams) => {
                            s3PutParams = putParams;
                            return Promise.resolve();
                        }
                    }
                })
            }
        ]
    });

    const processor = new Processor(context,
        // Set up with opConfig for first test
        {
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
        },
        {
            name: 's3_exporter'
        });

    const { worker } = processor;

    const slice = [DataEntity.make({
        field0: 0,
        field1: 1,
        field2: 2,
        field3: 3,
        field4: 4,
        field5: 5
    })];

    const emptySlice = [DataEntity.make(null)];

    const rawSlice = [DataEntity.make({ data: 'This is a sentence.' })];

    beforeAll(async () => {
        await processor.initialize();
    });

    afterAll(async () => {
        await processor.shutdown();
        context.apis.foundation.getSystemEvents().removeAllListeners();
    });

    it('initializes a slice counter.', () => {
        // Initialized to -1 so this can be incremented before slice processing
        expect(processor.sliceCount).toEqual(-1);
    });

    it('generates a csv object', async () => {
        await processor.onBatch(slice);

        expect(s3PutParams.Body).toEqual('0,1,2,3,4\n');
        expect(s3PutParams.Key).toEqual(`testing/${worker}.0`);
        expect(s3PutParams.Bucket).toEqual('data-store');
    });

    it('generates a tsv object', async () => {
        // This also makes sure the processor coerces the field delimiter
        processor.opConfig.format = 'tsv';
        await processor.onBatch(slice);

        expect(s3PutParams.Body).toEqual('0\t1\t2\t3\t4\n');
        expect(s3PutParams.Key).toEqual(`testing/${worker}.1`);
        expect(s3PutParams.Bucket).toEqual('data-store');
    });

    it('generates a tsv/csv object with an empty slice', async () => {
        await processor.onBatch(emptySlice);
        // Should be equal to the previous test since this will exit instead of calling the mock
        // writer to update the `s3PutParams` state
        expect(s3PutParams.Body).toEqual('0\t1\t2\t3\t4\n');
        expect(s3PutParams.Key).toEqual(`testing/${worker}.1`);
    });

    it('generates a raw object', async () => {
        processor.opConfig.format = 'raw';
        await processor.onBatch(rawSlice);
        expect(s3PutParams.Body).toEqual('This is a sentence.\n');
        // The previous test will still increment the slice count
        expect(s3PutParams.Key).toEqual(`testing/${worker}.3`);
    });

    it('generates an ldjson object and exludes a field', async () => {
        processor.opConfig.format = 'ldjson';
        await processor.onBatch(slice);
        expect(s3PutParams.Body).toEqual('{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4}\n');
        expect(s3PutParams.Key).toEqual(`testing/${worker}.4`);
    });

    it('generates an ldjson object', async () => {
        processor.opConfig.fields = [];
        await processor.onBatch(slice);
        expect(s3PutParams.Body).toEqual('{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4,"field5":5}\n');
        expect(s3PutParams.Key).toEqual(`testing/${worker}.5`);
    });

    it('generates a json object', async () => {
        processor.opConfig.format = 'json';
        await processor.onBatch(slice);
        expect(s3PutParams.Body).toEqual('[{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4,"field5":5}]\n');
        expect(s3PutParams.Key).toEqual(`testing/${worker}.6`);
    });

    it('generates lz4 compressed object', async () => {
        processor.opConfig.format = 'json';
        processor.opConfig.compression = 'lz4';
        await processor.onBatch(slice);
        expect(lz4.decode(Buffer.from(s3PutParams.Body)).toString()).toEqual('[{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4,"field5":5}]\n');
        // No file extensions since not configures in opConfig
        expect(s3PutParams.Key).toEqual(`testing/${worker}.7`);
    });

    it('generates gzip compressed object', async () => {
        processor.opConfig.format = 'json';
        processor.opConfig.compression = 'gzip';
        await processor.onBatch(slice);
        const decompressedObj = await ungzip(Buffer.from(s3PutParams.Body))
            .then((uncompressed) => uncompressed.toString());
        expect(decompressedObj).toEqual('[{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4,"field5":5}]\n');
        // No file extensions since not configures in opConfig
        expect(s3PutParams.Key).toEqual(`testing/${worker}.8`);
    });
});
