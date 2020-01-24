'use strict';

const { DataEntity } = require('@terascope/utils');
const { WorkerTestHarness } = require('teraslice-test-harness');

describe('Timeseries path partitioner', () => {
    let harness;

    const data = [
        DataEntity.make(
            {
                date: '2020-01-17T19:21:52.159Z',
                text: 'test data'
            }
        )
    ];

    afterEach(async () => {
        await harness.shutdown();
    });

    it('properly adds a daily path', async () => {
        harness = WorkerTestHarness.testProcessor({
            _op: 'timeseries_path_partitioner',
            base_path: '/data',
            date_field: 'date',
            prefix: '',
            type: 'daily'
        }, {});
        await harness.initialize();
        const slice = await harness.runSlice(data);
        // expect(results).toEqual(data);
        expect(slice[0].getMetadata('file:partition')).toEqual('/data/2020.01.17/');
    });

    it('properly adds a monthly path', async () => {
        harness = WorkerTestHarness.testProcessor({
            _op: 'timeseries_path_partitioner',
            base_path: '/data',
            date_field: 'date',
            prefix: '',
            type: 'monthly'
        }, {});
        await harness.initialize();
        const slice = await harness.runSlice(data);
        expect(slice[0].getMetadata('file:partition')).toEqual('/data/2020.01/');
    });

    it('properly adds a yearly path', async () => {
        harness = WorkerTestHarness.testProcessor({
            _op: 'timeseries_path_partitioner',
            base_path: '/data',
            date_field: 'date',
            prefix: '',
            type: 'yearly'
        }, {});
        await harness.initialize();
        const slice = await harness.runSlice(data);
        expect(slice[0].getMetadata('file:partition')).toEqual('/data/2020/');
    });
});
