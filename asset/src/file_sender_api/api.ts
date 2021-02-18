import {
    APIFactory, AnyObject, isNil, isString, getTypeOf
} from '@terascope/job-components';
import { FileSender } from '@terascope/file-asset-apis';
import { FileSenderAPIConfig } from './interfaces';

export default class FileSenderAPI extends APIFactory<FileSender, FileSenderAPIConfig> {
    validateConfig(input: AnyObject): FileSenderAPIConfig {
        if (isNil(input.path) || !isString(input.path)) throw new Error(`Invalid parameter path: it must be of type string, was given ${getTypeOf(input.path)}`);
        const workerId = this.context.cluster.worker.id;
        input.worker_id = workerId;
        return input as FileSenderAPIConfig;
    }

    async create(
        _name: string, overrideConfigs: Partial<FileSenderAPIConfig> = {}
    ):Promise<{ client: FileSender, config: FileSenderAPIConfig }> {
        const config = this.validateConfig(
            Object.assign({}, this.apiConfig, overrideConfigs)
        );
        // this is deprecated, used by routed-sender, please use dynamic_routing instead
        if (config._key) {
            config.dynamic_routing = true;
        }

        const client = new FileSender(config, this.logger);
        return { client, config };
    }

    async remove(_name: string): Promise<void> {}
}
