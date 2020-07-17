import {
    APIFactory, AnyObject, isNil, isString, getTypeOf
} from '@terascope/job-components';
import FileReader from './reader';
import { FileReaderAPIConfig } from './interfaces';

export default class FileReaderApi extends APIFactory<FileReader, FileReaderAPIConfig> {
    validateConfig(input: AnyObject): FileReaderAPIConfig {
        if (isNil(input.path) || !isString(input.path)) throw new Error(`Invalid parameter path: it must be of type string, was given ${getTypeOf(input.path)}`);
        return input as FileReaderAPIConfig;
    }

    async create(
        _name: string, overrideConfigs: Partial<FileReaderAPIConfig>
    ):Promise<{ client: FileReader, config: FileReaderAPIConfig }> {
        const config = this.validateConfig(Object.assign({}, this.apiConfig, overrideConfigs));
        const tryFn = this.tryRecord.bind(this);
        const rejectFn = this.rejectRecord.bind(this);
        const chunckedConfig = Object.assign(config, { tryFn, rejectFn });

        const client = new FileReader(chunckedConfig, this.logger);
        return { client, config };
    }

    async remove(_name: string): Promise<void> {}
}
