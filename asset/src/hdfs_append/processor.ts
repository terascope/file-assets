import { BatchProcessor, DataEntity } from '@terascope/job-components';
import { HDFSConfig } from './interfaces';
import { HDFSSenderFactoryAPI } from '../hdfs_sender_api/interfaces';
import HDFSSender from '../hdfs_sender_api/sender';

export default class HDFSBatcher extends BatchProcessor<HDFSConfig> {
    api!: HDFSSender;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<HDFSSenderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName, {} as any);
    }

    async onBatch(slice: DataEntity[]): Promise<DataEntity[]> {
        await this.api.send(slice);
        return slice;
    }
}
