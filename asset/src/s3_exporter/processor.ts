import { BatchProcessor, DataEntity } from '@terascope/job-components';
import { S3Sender } from '@terascope/file-asset-apis';
import { S3ExportConfig } from './interfaces';
import { S3SenderFactoryAPI } from '../s3_sender_api/interfaces';

export default class S3Batcher extends BatchProcessor<S3ExportConfig> {
    api!: S3Sender;
    private s3RecordsProcessed = 0;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<S3SenderFactoryAPI>(apiName);
        // this processor does not allow dynamic routing, use routed-sender operation instead
        this.api = await apiManager.create(apiName, { dynamic_routing: false });
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        const self = this;
        await this.context.apis.foundation.promMetrics.addGauge(
            'records_processed_from_s3',
            'Number of records written into s3',
            ['class'],
            async function collect() {
                const labels = {
                    class: 'S3Batcher',
                    ...self.context.apis.foundation.promMetrics.getDefaultLabels()
                };
                this.set(labels, self.getTotalProcessedS3Records());
            });
    }

    async onBatch(slice: DataEntity[]): Promise<DataEntity[]> {
        this.s3RecordsProcessed += slice.length;
        await this.api.send(slice);
        return slice;
    }

    getTotalProcessedS3Records(): number {
        return this.s3RecordsProcessed;
    }
}
