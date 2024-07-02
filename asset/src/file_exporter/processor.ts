import { BatchProcessor, DataEntity } from '@terascope/job-components';
import { FileSender } from '@terascope/file-asset-apis';
import { FileExporterConfig } from './interfaces.js';
import { FileSenderFactoryAPI } from '../file_sender_api/interfaces.js';

export default class FileBatcher extends BatchProcessor<FileExporterConfig> {
    api!: FileSender;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<FileSenderFactoryAPI>(apiName);
        // this processor does not allow dynamic routing, use routed-sender operation instead
        this.api = await apiManager.create(apiName, { dynamic_routing: false });
    }

    async onBatch(slice: DataEntity[]): Promise<DataEntity[]> {
        await this.api.send(slice);
        return slice;
    }
}
