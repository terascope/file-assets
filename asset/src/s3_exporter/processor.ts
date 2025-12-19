import { DataEntity } from '@terascope/core-utils';
import { BatchProcessor, isPromAvailable } from '@terascope/job-components';
import { S3Sender } from '@terascope/file-asset-apis';
import { S3ExportConfig } from './interfaces.js';
import { S3SenderFactoryAPI } from '../s3_sender_api/interfaces.js';

export default class S3Batcher extends BatchProcessor<S3ExportConfig> {
    api!: S3Sender;
    private s3RecordsWritten = 0;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiName = this.opConfig._api_name as string;
        const apiManager = this.getAPI<S3SenderFactoryAPI>(apiName);
        // this processor does not allow dynamic routing, use routed-sender operation instead
        this.api = await apiManager.create(apiName, { dynamic_routing: false });
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        const self = this;
        if (isPromAvailable(this.context)) {
            await this.context.apis.foundation.promMetrics.addGauge(
                'records_written_to_s3',
                'Number of records written into s3',
                ['op_name'],
                async function collect() {
                    const labels = {
                        op_name: 's3_exporter',
                        ...self.context.apis.foundation.promMetrics.getDefaultLabels()
                    };
                    this.set(labels, self.getTotalWrittenS3Records());
                });
        }
    }

    async onBatch(slice: DataEntity[]): Promise<DataEntity[]> {
        this.s3RecordsWritten += slice.length;
        await this.api.send(slice);
        return slice;
    }

    getTotalWrittenS3Records(): number {
        return this.s3RecordsWritten;
    }
}
