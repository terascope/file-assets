import {
    APIFactory, AnyObject, isNil, isString, getTypeOf
} from '@terascope/job-components';
import { S3Reader } from '@terascope/file-asset-apis';
import { S3ReaderAPIConfig } from './interfaces';

export default class S3ReaderAPI extends APIFactory<S3Reader, S3ReaderAPIConfig> {
    validateConfig(input: AnyObject): S3ReaderAPIConfig {
        if (isNil(input.path) || !isString(input.path)) throw new Error(`Invalid parameter path: it must be of type string, was given ${getTypeOf(input.path)}`);
        return input as S3ReaderAPIConfig;
    }

    async create(
        _name: string, overrideConfigs: Partial<S3ReaderAPIConfig>
    ):Promise<{ client: S3Reader, config: S3ReaderAPIConfig }> {
        const config = this.validateConfig(Object.assign({}, this.apiConfig, overrideConfigs));
        const s3Client = this.context.foundation.getConnection({
            endpoint: config.connection,
            type: 's3',
            cached: true
        }).client;
        const tryFn = this.tryRecord.bind(this);
        const rejectFn = this.rejectRecord.bind(this);
        const chunkedConfig = Object.assign(config, { tryFn, rejectFn });

        const client = new S3Reader(s3Client, chunkedConfig, this.logger);
        return { client, config };
    }

    async remove(_name: string): Promise<void> {}
}
