import { BatchProcessor, DataEntity } from '@terascope/job-components';
import { FileExporterConfig } from './interfaces';
import FileSender from '../file_sender_api/sender';
import { FileSenderFactoryAPI } from '../file_sender_api/interfaces';

export default class FileBatcher extends BatchProcessor<FileExporterConfig> {
    api!: FileSender;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<FileSenderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName, {} as any);
    }

    async onBatch(slice: DataEntity[]): Promise<DataEntity[]> {
        await this.api.send(slice);
        return slice;
    }
}
