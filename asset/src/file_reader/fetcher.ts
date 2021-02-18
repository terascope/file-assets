import { Fetcher, DataEntity } from '@terascope/job-components';
import { FileReader, SlicedFileResults } from '@terascope/file-asset-apis';
import { FileReaderConfig } from './interfaces';
import { FileReaderFactoryAPI } from '../file_reader_api/interfaces';

export default class FileFetcher extends Fetcher<FileReaderConfig> {
    api!: FileReader

    async initialize(): Promise<void> {
        await super.initialize();
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<FileReaderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName, {});
    }

    async fetch(slice: SlicedFileResults): Promise<DataEntity[]> {
        return this.api.read(slice);
    }
}
