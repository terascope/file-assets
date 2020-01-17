'use strict';

const { TestContext } = require('@terascope/job-components');
const { DataEntity } = require('@terascope/utils');
const Processor = require('../../asset/timeseries_path_partitioner/processor');

describe('Timeseries router', () => {
    const context = new TestContext('timeseries-path-partitioner');
    // This sets up the opconfig for the first test
    const processor = new Processor(context,
        {
            _op: 'timeseries_path_partitioner',
            base_path: '/data',
            date_field: 'date',
            prefix: '',
            type: 'daily'
        },
        {
            name: 'timeseries_path_partitioner'
        });

    const data = [
        DataEntity.make(
            {
                date: '2020-01-17T19:21:52.159Z',
                text: 'test data'
            }
        )
    ];

    beforeAll(async () => {
        await processor.initialize();
    });

    afterAll(async () => {
        await processor.shutdown();
        context.apis.foundation.getSystemEvents().removeAllListeners();
    });

    it('properly adds a daily path', async () => {
        const slice = await processor.onBatch(data);
        // expect(processor.toEqual(1))
        expect(slice[0].getMetadata('routingPath')).toEqual('/data/2020.01.17/');
    });
    it('properly adds a monthly path', async () => {
        processor.opConfig.type = 'monthly';
        const slice = await processor.onBatch(data);
        // expect(processor.toEqual(1))
        expect(slice[0].getMetadata('routingPath')).toEqual('/data/2020.01/');
    });
    it('properly adds a yearly path', async () => {
        processor.opConfig.type = 'yearly';
        const slice = await processor.onBatch(data);
        // expect(processor.toEqual(1))
        expect(slice[0].getMetadata('routingPath')).toEqual('/data/2020/');
    });
});
