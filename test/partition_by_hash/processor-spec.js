'use strict';

const { DataEntity } = require('@terascope/utils');
const { WorkerTestHarness } = require('teraslice-test-harness');

describe('Date path partitioner', () => {
    let harness;

    const data = [
        DataEntity.make(
            {
                date: '2020-01-17T19:21:52.159Z',
                field1: 'val1.1',
                field2: 'val1.2'
            }
        ),
        DataEntity.make(
            {
                date: '2020-01-17T19:21:52.159Z',
                field1: 'val2.1',
                field2: 'val2.2'
            }
        ),
    ];

    afterEach(async () => {
        await harness.shutdown();
    });

    it('properly adds partition with specified keys', async () => {
        harness = WorkerTestHarness.testProcessor({
            _op: 'partition_by_hash',
            path: '/data',
            fields: [
                'field2',
                'field1'
            ],
            partitions: 15
        }, {});
        await harness.initialize();
        const slice = await harness.runSlice(data);
        expect(slice[0].getMetadata('file:partition')).toEqual('/data/partition=11/');
        expect(slice[1].getMetadata('file:partition')).toEqual('/data/partition=13/');
    });
});
