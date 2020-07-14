import {
    APIFactory, AnyObject, isNil, isString, getTypeOf
} from '@terascope/job-components';
import S3Reader from './reader';
import { S3ReaderApi } from './interfaces';

export default class S3ReaderAPI extends APIFactory<S3Reader, S3ReaderApi> {
    validateConfig(input: AnyObject): S3ReaderApi {
        if (isNil(input.path) || !isString(input.path)) throw new Error(`Invalid parameter path: it must be of type string, was given ${getTypeOf(input.path)}`);
        return input as S3ReaderApi;
    }

    async create(
        _name: string, overrideConfigs: Partial<S3ReaderApi>
    ):Promise<{ client: S3Reader, config: S3ReaderApi }> {
        const config = this.validateConfig(Object.assign({}, this.apiConfig, overrideConfigs));
        const s3Client = this.context.foundation.getConnection({
            endpoint: config.connection,
            type: 's3',
            cached: true
        }).client;

        const client = new S3Reader(s3Client, config, this.logger);
        return { client, config };
    }

    async remove(_name: string): Promise<void> {}
}
