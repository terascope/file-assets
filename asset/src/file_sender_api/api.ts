import { APIFactory, AnyObject } from '@terascope/job-components';
import FileSender from './sender';
import { ReaderFileAPI } from './interfaces';

export default class FileReaderApi extends APIFactory<FileSender, ReaderFileAPI> {
    validateConfig(input: AnyObject): ReaderFileAPI {
        // TODO: check configs
        const workerId = this.context.cluster.worker.id;
        input.workerId = workerId;
        return input as ReaderFileAPI;
    }

    async create(
        _name: string, overrideConfigs: Partial<ReaderFileAPI>
    ):Promise<{ client: FileSender, config: ReaderFileAPI }> {
        const config = this.validateConfig(
            Object.assign({}, this.apiConfig, overrideConfigs)
        );
        const client = new FileSender(config, this.logger);
        return { client, config };
    }

    async remove(_name: string): Promise<void> {}
}
