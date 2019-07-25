'use strict';

const { TestContext } = require('@terascope/job-components');
const Processor = require('../../asset/s3_exporter/processor');

describe('S3 exporter processor', () => {
    const context = new TestContext('s3-exporter');
    const processor = new Processor(context,
        // Set up with opConfig for first test
        {
            _op: 's3_exporter',
            bucket: 'data-store',
            connection: 'my-s3-connector',
            object_prefix: 'testing/',
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

    const { workerId } = processor;

    // Fake the client for testing
    processor.client = {
        putObject_Async: putParams => putParams
    };

    const slice = [{
        field0: 0,
        field1: 1,
        field2: 2,
        field3: 3,
        field4: 4,
        field5: 5
    }];

    const emptySlice = [null];

    const rawSlice = [{
        data: 'This is a sentence.'
    }];

    afterAll(() => {
        context.apis.foundation.getSystemEvents().removeAllListeners();
    });

    it('initializes a slice counter.', () => {
        expect(processor.sliceCount).toEqual(0);
    });

    it('generates a csv object', async () => {
        const objParams = await processor.onBatch(slice);

        expect(objParams.Body).toEqual('0,1,2,3,4\n');
        expect(objParams.Key).toEqual(`testing/${workerId}.0`);
        expect(objParams.Bucket).toEqual('data-store');
    });

    it('generates a tsv object', async () => {
        // This also makes sure the processor coerces the field delimiter
        processor.opConfig.format = 'tsv';
        const objParams = await processor.onBatch(slice);

        expect(objParams.Body).toEqual('0\t1\t2\t3\t4\n');
        expect(objParams.Key).toEqual(`testing/${workerId}.1`);
        expect(objParams.Bucket).toEqual('data-store');
    });

    it('generates a tsv/csv object with an empty slice', async () => {
        const objParams = await processor.onBatch(emptySlice);

        expect(objParams.Body).toEqual('\n');
        expect(objParams.Key).toEqual(`testing/${workerId}.2`);
    });

    it('generates a raw object', async () => {
        processor.opConfig.format = 'raw';
        const objParams = await processor.onBatch(rawSlice);
        expect(objParams.Body).toEqual('This is a sentence.\n');
        expect(objParams.Key).toEqual(`testing/${workerId}.3`);
    });

    it('generates an ldjson object and exludes a field', async () => {
        processor.opConfig.format = 'ldjson';
        const objParams = await processor.onBatch(slice);
        expect(objParams.Body).toEqual('{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4}\n');
        expect(objParams.Key).toEqual(`testing/${workerId}.4`);
    });

    it('generates an ldjson object', async () => {
        processor.opConfig.fields = [];
        const objParams = await processor.onBatch(slice);
        expect(objParams.Body).toEqual('{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4,"field5":5}\n');
        expect(objParams.Key).toEqual(`testing/${workerId}.5`);
    });

    it('generates a json object', async () => {
        processor.opConfig.format = 'json';
        const objParams = await processor.onBatch(slice);
        expect(objParams.Body).toEqual('[{"field0":0,"field1":1,"field2":2,"field3":3,"field4":4,"field5":5}]\n');
        expect(objParams.Key).toEqual(`testing/${workerId}.6`);
    });
});
