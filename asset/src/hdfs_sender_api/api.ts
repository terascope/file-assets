import {
    APIFactory, AnyObject, isNil, isString, getTypeOf
} from '@terascope/job-components';
import HDFSSender from './sender';
import { HDFSExporterAPIConfig } from './interfaces';

export default class HDFSSenderFactoryApi extends APIFactory<HDFSSender, HDFSExporterAPIConfig> {
    validateConfig(input: AnyObject): HDFSExporterAPIConfig {
        if (isNil(input.path) || !isString(input.path)) throw new Error(`Invalid parameter path: it must be of type string, was given ${getTypeOf(input.path)}`);
        const workerId = this.context.cluster.worker.id;
        input.workerId = workerId;
        return input as HDFSExporterAPIConfig;
    }

    async create(
        _name: string, overrideConfigs: Partial<HDFSExporterAPIConfig>
    ):Promise<{ client: HDFSSender, config: HDFSExporterAPIConfig }> {
        const config = this.validateConfig(
            Object.assign({}, this.apiConfig, overrideConfigs)
        );
        const hdfsClient = this.context.foundation.getConnection({
            endpoint: config.connection,
            type: 'hdfs_ha',
            cached: false
        });
        const client = new HDFSSender(hdfsClient, config, this.logger);
        return { client, config };
    }

    async remove(_name: string): Promise<void> {}
}
