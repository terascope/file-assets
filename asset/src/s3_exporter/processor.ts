import {
    BatchProcessor,
    ExecutionConfig,
    WorkerContext,
    DataEntity,
    isEmpty,
} from '@terascope/job-components';
import { S3ExportConfig } from './interfaces';
import { NameOptions } from '../__lib/interfaces';
import S3Sender from '../s3_sender_api/sender';
import { S3SenderFactoryAPI } from '../s3_sender_api/interfaces';

export default class S3Batcher extends BatchProcessor<S3ExportConfig> {
    api!: S3Sender;
    nameOptions: NameOptions;

    constructor(context: WorkerContext, opConfig: S3ExportConfig, exConfig: ExecutionConfig) {
        super(context, opConfig, exConfig);
        const extension = isEmpty(opConfig.extension) ? undefined : opConfig.extension;

        // `filePerSlice` needs to be ignored since you cannot append to S3 objects
        this.nameOptions = {
            filePath: opConfig.path,
            extension,
            filePerSlice: true
        };
    }

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
