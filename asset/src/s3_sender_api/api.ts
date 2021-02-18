import {
    APIFactory, AnyObject, isNil, isString, getTypeOf
} from '@terascope/job-components';
import { S3Sender } from '@terascope/file-asset-apis';
import { S3ExporterAPIConfig } from './interfaces';

export default class S3SenderAPI extends APIFactory<S3Sender, S3ExporterAPIConfig> {
    validateConfig(input: AnyObject): S3ExporterAPIConfig {
        if (isNil(input.path) || !isString(input.path)) throw new Error(`Invalid parameter path: it must be of type string, was given ${getTypeOf(input.path)}`);
        const workerId = this.context.cluster.worker.id;
        input.worker_id = workerId;
        // file_per_slice must be set to true if compression is set to anything besides "none"
        if (input.compression !== 'none' && input.file_per_slice !== true) {
            throw new Error('Invalid parameter "file_per_slice", it must be set to true if compression is set to anything other than "none" as we cannot properly divide up a compressed file');
        }
        return input as S3ExporterAPIConfig;
    }

    async create(
        _name: string, overrideConfigs: Partial<S3ExporterAPIConfig>
    ):Promise<{ client: S3Sender, config: S3ExporterAPIConfig }> {
        const config = this.validateConfig(
            Object.assign({}, this.apiConfig, overrideConfigs)
        );

        // this is deprecated, used by routed-sender, please use dynamic_routing instead
        if (config._key) {
            config.dynamic_routing = true;
        }

        const s3Client = this.context.foundation.getConnection({
            endpoint: config.connection,
            type: 's3',
            cached: true
        }).client;

        const client = new S3Sender(s3Client, config, this.logger);

        await client.ensureBucket(config.path);

        return { client, config };
    }

    async remove(_name: string): Promise<void> {}
}
