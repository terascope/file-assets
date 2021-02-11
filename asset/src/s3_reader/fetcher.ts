import { Fetcher, DataEntity } from '@terascope/job-components';
import { SlicedFileResults, S3Reader } from '@terascope/file-asset-apis';
import { S3ReaderConfig } from './interfaces';
import { S3ReaderFactoryAPI } from '../s3_reader_api/interfaces';

export default class S3Fetcher extends Fetcher<S3ReaderConfig> {
    api!: S3Reader

    async initialize(): Promise<void> {
        await super.initialize();
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<S3ReaderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName, {} as any);
    }

    async fetch(slice: SlicedFileResults): Promise<DataEntity[]> {
        return this.api.read(slice);
    }
}
