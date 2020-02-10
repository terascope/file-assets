'use strict';

const { DataEntity } = require('@terascope/utils');
const { WorkerTestHarness } = require('teraslice-test-harness');

describe('Date path partitioner', () => {
    let harness;

    const data = [
        DataEntity.make(
            {
                date: '2020-01-17T19:21:52.159Z',
                field1: 'val1',
                field2: 'val2'
            }
        ),
        DataEntity.make(
            {
                date: '2020-01-17T19:21:52.159Z',
                field1: 'val/1',
                field2: 'val/2'
            }
        ),
        DataEntity.make(
            {
                date: '2020-01-17T19:21:52.159Z',
                field1: 'val=1',
                field2: 'val=2'
            }
        )
    ];

    afterEach(async () => {
        await harness.shutdown();
    });

    it('properly adds partition with specified keys', async () => {
        harness = WorkerTestHarness.testProcessor({
            _op: 'partition_by_fields',
            path: '/data',
            fields: [
                'field2',
                'field1'
            ]
        }, {});
        await harness.initialize();
        const slice = await harness.runSlice(data);
        expect(slice[0].getMetadata('file:partition')).toEqual('/data/field2=val2/field1=val1/');
        expect(slice[1].getMetadata('file:partition')).toEqual('/data/field2=val_2/field1=val_1/');
        expect(slice[2].getMetadata('file:partition')).toEqual('/data/field2=val_2/field1=val_1/');
    });
});
