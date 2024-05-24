import { Fetcher, DataEntity, isPromAvailable } from '@terascope/job-components';
import { FileSlice, S3TerasliceAPI } from '@terascope/file-asset-apis';
import { S3ReaderConfig } from './interfaces';
import { S3ReaderFactoryAPI } from '../s3_reader_api/interfaces';

export default class S3Fetcher extends Fetcher<S3ReaderConfig> {
    api!: S3TerasliceAPI;
    private s3RecordsRead = 0;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<S3ReaderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName, {} as any);
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        const self = this;
        if (isPromAvailable(this.context)) {
            await this.context.apis.foundation.promMetrics.addGauge(
                'records_read_from_s3',
                'Number of records read from s3',
                ['op_name'],
                async function collect() {
                    const labels = {
                        op_name: 's3_reader',
                        ...self.context.apis.foundation.promMetrics.getDefaultLabels()
                    };
                    this.set(labels, self.getTotalReadS3Records());
                });
        }
    }

    async fetch(slice: FileSlice): Promise<DataEntity[]> {
        const data = await this.api.read(slice);
        this.s3RecordsRead += data.length;
        return data;
    }

    getTotalReadS3Records(): number {
        return this.s3RecordsRead;
    }
}
