import {
    APIFactory, AnyObject, isNil, isString, getTypeOf
} from '@terascope/job-components';
import { HDFSSender } from '@terascope/file-asset-apis';
import { HDFSExporterAPIConfig } from './interfaces';

export default class HDFSSenderFactoryAPI extends APIFactory<HDFSSender, HDFSExporterAPIConfig> {
    validateConfig(input: AnyObject): HDFSExporterAPIConfig {
        if (isNil(input.path) || !isString(input.path)) throw new Error(`Invalid parameter path: it must be of type string, was given ${getTypeOf(input.path)}`);
        const workerId = this.context.cluster.worker.id;
        input.worker_id = workerId;
        return input as HDFSExporterAPIConfig;
    }

    async create(
        _name: string, overrideConfigs: Partial<HDFSExporterAPIConfig>
    ):Promise<{ client: HDFSSender, config: HDFSExporterAPIConfig }> {
        const config = this.validateConfig(
            Object.assign({}, this.apiConfig, overrideConfigs)
        );

        // this is deprecated, used by routed-sender, please use dynamic_routing instead
        if (config._key) {
            config.dynamic_routing = true;
        }

        const hdfsClient = this.context.foundation.getConnection({
            endpoint: config.connection,
            type: 'hdfs_ha',
            cached: false
        }).client;

        const client = new HDFSSender(hdfsClient, config, this.logger);
        return { client, config };
    }

    async remove(_name: string): Promise<void> {}
}
