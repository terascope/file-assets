import {
    APIFactory, AnyObject, isNil, isString, getTypeOf
} from '@terascope/job-components';
import { FileTerasliceAPI } from '@terascope/file-asset-apis';
import { FileReaderAPIConfig } from './interfaces';

export default class FileReaderAPI extends APIFactory<FileTerasliceAPI, FileReaderAPIConfig> {
    validateConfig(input: AnyObject): FileReaderAPIConfig {
        if (isNil(input.path) || !isString(input.path)) throw new Error(`Invalid parameter path: it must be of type string, was given ${getTypeOf(input.path)}`);
        // file_per_slice must be set to true if compression is set to anything besides "none"
        if (input.compression !== 'none' && input.file_per_slice !== true) {
            throw new Error('Invalid parameter "file_per_slice", it must be set to true if compression is set to anything other than "none" as we cannot properly divide up a compressed file');
        }
        return input as FileReaderAPIConfig;
    }

    async create(
        _name: string, overrideConfigs: Partial<FileReaderAPIConfig>
    ):Promise<{ client: FileTerasliceAPI, config: FileReaderAPIConfig }> {
        const config = this.validateConfig(Object.assign({}, this.apiConfig, overrideConfigs));
        const tryFn = this.tryRecord.bind(this);
        const rejectFn = this.rejectRecord.bind(this);
        const chunkedConfig = Object.assign(config, { tryFn, rejectFn });

        const client = new FileTerasliceAPI(chunkedConfig, this.logger);
        return { client, config };
    }

    async remove(_name: string): Promise<void> {}
}
