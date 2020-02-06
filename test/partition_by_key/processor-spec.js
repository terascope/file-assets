'use strict';

const { DataEntity } = require('@terascope/utils');
const { WorkerTestHarness } = require('teraslice-test-harness');

describe('Key path partitioner', () => {
    let harness;

    const data = [
        DataEntity.make(
            {
                date: '2020-01-17T19:21:52.159Z',
                text: 'test data'
            },
            {
                _key: 'DaTaEnTiTyK3Y'
            }
        ),
        DataEntity.make(
            {
                date: '2020-01-17T19:21:52.159Z',
                text: 'test data'
            },
            {
                _key: 'DaTaEnTiTy/K3Y'
            }
        ),
        DataEntity.make(
            {
                date: '2020-01-17T19:21:52.159Z',
                text: 'test data'
            },
            {
                _key: 'DaTaEnTiTy=K3Y'
            }
        )
    ];

    afterEach(async () => {
        await harness.shutdown();
    });

    it('properly adds the key to the path', async () => {
        harness = WorkerTestHarness.testProcessor({
            _op: 'partition_by_key',
            path: '/data',
        }, {});
        await harness.initialize();
        // Need this in order to feed the record in with the metadata
        harness.fetcher().handle = () => data;
        const slice = await harness.runSlice(data);
        expect(slice[0].getMetadata('file:partition')).toEqual('/data/_key=DaTaEnTiTyK3Y/');
        expect(slice[1].getMetadata('file:partition')).toEqual('/data/_key=DaTaEnTiTy_K3Y/');
        expect(slice[1].getMetadata('file:partition')).toEqual('/data/_key=DaTaEnTiTy_K3Y/');
    });
});
