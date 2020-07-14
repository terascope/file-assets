import {
    APIFactory, AnyObject, isNil, isString, getTypeOf
} from '@terascope/job-components';
import S3Sender from './sender';
import { S3ExporterAPI } from './interfaces';

export default class FileReaderApi extends APIFactory<S3Sender, S3ExporterAPI> {
    validateConfig(input: AnyObject): S3ExporterAPI {
        if (isNil(input.path) || !isString(input.path)) throw new Error(`Invalid parameter path: it must be of type string, was given ${getTypeOf(input.path)}`);
        const workerId = this.context.cluster.worker.id;
        input.workerId = workerId;
        return input as S3ExporterAPI;
    }

    async create(
        _name: string, overrideConfigs: Partial<S3ExporterAPI>
    ):Promise<{ client: S3Sender, config: S3ExporterAPI }> {
        const config = this.validateConfig(
            Object.assign({}, this.apiConfig, overrideConfigs)
        );
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
