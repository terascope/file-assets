import {
    APIFactory, isNil, isString, AnyObject, getTypeOf
} from '@terascope/job-components';
import { HDFSReader } from '@terascope/file-asset-apis';
import { HDFSReaderApiConfig } from './interfaces';

export default class HDFSReaderFactoryAPI extends APIFactory<HDFSReader, HDFSReaderApiConfig> {
    validateConfig(input: AnyObject): HDFSReaderApiConfig {
        if (isNil(input.path) || !isString(input.path)) throw new Error(`Invalid parameter path: it must be of type string, was given ${getTypeOf(input.path)}`);
        // file_per_slice must be set to true if compression is set to anything besides "none"
        if (input.compression !== 'none' && input.file_per_slice !== true) {
            throw new Error('Invalid parameter "file_per_slice", it must be set to true if compression is set to anything other than "none" as we cannot properly divide up a compressed file');
        }
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
        const tryFn = this.tryRecord.bind(this);
        const rejectFn = this.rejectRecord.bind(this);
        const chunkedConfig = Object.assign({}, config, { tryFn, rejectFn });

        const client = new HDFSReader(s3Client, chunkedConfig, this.logger);
        return { client, config };
    }

    async remove(_name: string): Promise<void> {}
}
