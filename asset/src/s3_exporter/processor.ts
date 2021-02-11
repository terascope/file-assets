import { BatchProcessor, DataEntity } from '@terascope/job-components';
import { S3Sender } from '@terascope/file-asset-apis';
import { S3ExportConfig } from './interfaces';
import { S3SenderFactoryAPI } from '../s3_sender_api/interfaces';

export default class S3Batcher extends BatchProcessor<S3ExportConfig> {
    api!: S3Sender;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<S3SenderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName, {} as any);
    }

    async onBatch(slice: DataEntity[]): Promise<DataEntity[]> {
        await this.api.send(slice);
        return slice;
    }
}
