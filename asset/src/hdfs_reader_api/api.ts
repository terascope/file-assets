import {
    APIFactory, isNil, isString, AnyObject, getTypeOf
} from '@terascope/job-components';
import { HDFSReaderApiConfig } from './interfaces';
import HDFSReader from './reader';

export default class HDFSReaderFactoryAPI extends APIFactory<HDFSReader, HDFSReaderApiConfig> {
    validateConfig(input: AnyObject): HDFSReaderApiConfig {
        if (isNil(input.path) || !isString(input.path)) throw new Error(`Invalid parameter path: it must be of type string, was given ${getTypeOf(input.path)}`);
        return input as HDFSReaderApiConfig;
    }

    async create(
        _name: string, overrideConfigs: Partial<HDFSReaderApiConfig>
    ):Promise<{ client: HDFSReader, config: HDFSReaderApiConfig }> {
        const config = this.validateConfig(Object.assign({}, this.apiConfig, overrideConfigs));
        const s3Client = this.context.foundation.getConnection({
            endpoint: config.connection,
            type: 'hdfs_ha',
            cached: true
        }).client;
        const client = new HDFSReader(s3Client, config, this.logger);
        return { client, config };
    }

    async remove(_name: string): Promise<void> {}
}
