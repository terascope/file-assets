import { DataEntity } from '@terascope/core-utils';
import { BatchProcessor } from '@terascope/job-components';
import { HDFSSender } from '@terascope/file-asset-apis';
import { HDFSExportOpConfig } from './interfaces.js';
import { HDFSSenderFactoryAPI } from '../hdfs_sender_api/interfaces.js';

export default class HDFSBatcher extends BatchProcessor<HDFSExportOpConfig> {
    api!: HDFSSender;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiName = this.opConfig.api_name as string;
        const apiManager = this.getAPI<HDFSSenderFactoryAPI>(apiName);
        // this processor does not allow dynamic routing, use routed-sender operation instead
        this.api = await apiManager.create(apiName, { dynamic_routing: false });
    }

    async onBatch(slice: DataEntity[]): Promise<DataEntity[]> {
        await this.api.send(slice);
        return slice;
    }
}
