import {
    APIFactory, AnyObject, isNil, isString, getTypeOf
} from '@terascope/job-components';
import FileReader from './reader';
import { ReaderFileAPI } from './interfaces';

export default class FileReaderApi extends APIFactory<FileReader, ReaderFileAPI> {
    validateConfig(input: AnyObject): ReaderFileAPI {
        if (isNil(input.path) || !isString(input.path)) throw new Error(`Invalid parameter path: it must be of type string, was given ${getTypeOf(input.path)}`);
        return input as ReaderFileAPI;
    }

    async create(
        _name: string, overrideConfigs: Partial<ReaderFileAPI>
    ):Promise<{ client: FileReader, config: ReaderFileAPI }> {
        const config = this.validateConfig(Object.assign({}, this.apiConfig, overrideConfigs));
        const client = new FileReader(config, this.logger);
        return { client, config };
    }

    async remove(_name: string): Promise<void> {}
}
