import { Fetcher, DataEntity } from '@terascope/job-components';
import { HDFSReader, FileSlice } from '@terascope/file-asset-apis';
import { HDFSReaderOpConfig } from './interfaces.js';
import { HDFSReaderFactoryAPI } from '../hdfs_reader_api/interfaces.js';

export default class HDFSFetcher extends Fetcher<HDFSReaderOpConfig> {
    api!: HDFSReader;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiName = this.opConfig.api_name as string;
        const apiManager = this.getAPI<HDFSReaderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName, {} as any);
    }

    async fetch(slice: FileSlice): Promise<DataEntity[]> {
        return this.api.read(slice);
    }
}
