import { Fetcher, DataEntity } from '@terascope/job-components';
import { HDFSReaderConfig } from './interfaces';
import HDFSReader from '../hdfs_reader_api/reader';
import { HDFSReaderFactoryAPI } from '../hdfs_reader_api/interfaces';
import { SlicedFileResults } from '../__lib/interfaces';

export default class HDFSFetcher extends Fetcher<HDFSReaderConfig> {
    api!: HDFSReader

    async initialize(): Promise<void> {
        await super.initialize();
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<HDFSReaderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName, {} as any);
    }

    async fetch(slice: SlicedFileResults): Promise<DataEntity[]> {
        return this.api.read(slice);
    }
}
