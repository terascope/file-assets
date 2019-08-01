'use strict';

const { TestContext } = require('@terascope/job-components');
const Promise = require('bluebird');
const Fetcher = require('../../asset/s3_reader/fetcher');


// Set up a test response to feed the slicer.
// offset: 0
// length: 27
const response = {
    Body: 'this\tis\tsome\tcsv\ttest\tdata\n'
};

let s3Params = {};

// Set up a mock client to extract the generated S3 options
const mockClient = {
    getObject_Async: (params) => {
        s3Params = params;
        return Promise.resolve(response);
    }
};

const slice = {
    path: 'my/test/data.csv',
    offset: 0,
    length: 27
};

describe('S3 reader\'s slicer', () => {
    const context = new TestContext('s3-reader');
    // Make sure the JSON slices will be the whole files
    const fetcher = new Fetcher(context,
        // Set up with opConfig for first test
        {
            _op: 's3_reader',
            bucket: 'data-store',
            connection: 'my-s3-connector',
            object_prefix: 'my/test/',
            size: 27,
            format: 'tsv',
            field_delimiter: ',',
            line_delimiter: '\n'
        },
        {
            name: 's3_exporter'
        });

    // Set the clients to our test client
    fetcher.client = mockClient;

    it('generated the proper S3 getObject settings', async () => {
        const data = await fetcher.fetch(slice);
        expect(s3Params.Range).toEqual('bytes=0-26');
        expect(Object.keys(data[0]).length).toEqual(6);
    });
});
