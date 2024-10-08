import {
    APIFactory, AnyObject, isNil,
    isString, getTypeOf, get, toString
} from '@terascope/job-components';
import { HDFSSender } from '@terascope/file-asset-apis';
import { HDFSExporterAPIConfig } from './interfaces.js';

export default class HDFSSenderFactoryAPI extends APIFactory<HDFSSender, HDFSExporterAPIConfig> {
    validateConfig(input: AnyObject): HDFSExporterAPIConfig {
        if (isNil(input.path) || !isString(input.path)) throw new Error(`Invalid parameter path: it must be of type string, was given ${getTypeOf(input.path)}`);
        const workerId = toString(get(this.context, 'cluster.worker.id'));
        input.id = workerId;
        // file_per_slice must be set to true if compression is set to anything besides "none"
        if (input.compression !== 'none' && input.file_per_slice !== true) {
            throw new Error('Invalid parameter "file_per_slice", it must be set to true if compression is set to anything other than "none" as we cannot properly divide up a compressed file');
        }
        return input as HDFSExporterAPIConfig;
    }

    async create(
        _name: string, overrideConfigs: Partial<HDFSExporterAPIConfig>
    ): Promise<{ client: HDFSSender; config: HDFSExporterAPIConfig }> {
        const config = this.validateConfig(
            Object.assign({}, this.apiConfig, overrideConfigs)
        );

        // this is deprecated, used by routed-sender, please use dynamic_routing instead
        if (config._key) {
            config.dynamic_routing = true;
        }

        const { client: hdfsClient } = await this.context.apis.foundation.createClient({
            endpoint: config.connection,
            type: 'hdfs_ha',
            cached: false
        });

        const client = new HDFSSender(hdfsClient, config, this.logger);
        return { client, config };
    }

    async remove(_name: string): Promise<void> {}
}
