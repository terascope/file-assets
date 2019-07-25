'use strict';

const { TestContext } = require('@terascope/job-components');
const Slicer = require('../../asset/s3_reader/slicer');


// Set up a test response to feed the slicer. This will include an object larger than the slice,
// and two larger than the
const response = {
    IsTruncated: true,
    Contents: [
        {
            Key: 'obj0',
            Size: 1000
        },
        {
            Key: 'obj1',
            Size: 500
        },
        {
            Key: 'obj2',
            Size: 500
        }
    ]
};

// Set up a mock client and a toggle to let it trigger the slicer's additional list query
let firstRun = true;
const mockClient = {
    listObjects_Async: () => {
        if (firstRun) {
            firstRun = false;
            return response;
        }
        response.IsTruncated = false;
        return response;
    }
};

describe('S3 reader\'s slicer', () => {
    const context = new TestContext('s3-reader');
    // Make sure the JSON slices will be the whole files
    const slicerJSON = new Slicer(context,
        // Set up with opConfig for first test
        {
            _op: 's3_reader',
            bucket: 'data-store',
            connection: 'my-s3-connector',
            object_prefix: 'testing/',
            size: 500,
            format: 'json'
        },
        {
            name: 's3_exporter'
        });
    // Make sure non-JSON format slices files on slice size
    const slicerOther = new Slicer(context,
        // Set up with opConfig for first test
        {
            _op: 's3_reader',
            bucket: 'data-store',
            connection: 'my-s3-connector',
            object_prefix: 'testing/',
            size: 500,
            format: 'ldjson',
            line_delimiter: '\n'
        },
        {
            name: 's3_exporter'
        });

    // Set the clients to our test client
    slicerJSON.client = mockClient;
    slicerOther.client = mockClient;

    it('slices JSON objects into the expected number of slices.', async () => {
        slicerJSON.initialize();
        // Make sure the slicer has time to slice
        setTimeout(() => {
            expect(slicerJSON._queue._size).toEqual(6);
            expect(slicerJSON._doneSlicing).toEqual(true);
            expect(slicerJSON._lastKey).toEqual('obj2');
        }, 1000);
    });


    it('slices non-JSON objects correctly', async () => {
        // Reset the test parameters
        firstRun = true;
        response.IsTruncated = true;
        slicerOther.initialize();
        // Make sure the slicer has time to slice
        setTimeout(async () => {
            await expect(slicerOther._queue._size).toEqual(8);
            await expect(slicerOther._doneSlicing).toEqual(true);
            // slices will come out in order since they are synchronously loaded into the internal
            // queue. Also this check needs to happen here since it will unload the queue in the
            // middle of the other test
            const slice1 = await slicerOther.slice();
            const slice2 = await slicerOther.slice();
            const slice3 = await slicerOther.slice();
            const slice4 = await slicerOther.slice();
            expect(slice1.offset).toEqual(0);
            expect(slice2.offset).toEqual(499);
            expect(slice3.offset).toEqual(0);
            expect(slice4.offset).toEqual(0);
        }, 1000);
    });
});
