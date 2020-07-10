import {
    ConvictSchema,
    ValidatedJobConfig,
    getOpConfig,
    isNil,
    cloneDeep,
    AnyObject,
    isString,
    getTypeOf,
    isEmpty,
    isNotNil
} from '@terascope/job-components';
import { FileReaderConfig } from './interfaces';
import { fileReaderSchema } from '../__lib/common-schema';
import { DEFAULT_API_NAME } from '../file_reader_api/interfaces';

const clonedSchema = cloneDeep(fileReaderSchema) as AnyObject;

clonedSchema.api_name = {
    doc: 'name of api to be used by elasticearch reader',
    default: DEFAULT_API_NAME,
    format: (val: unknown): void => {
        if (!isString(val)) throw new Error(`Invalid parameter api_name, it must be of type string, was given ${getTypeOf(val)}`);
        if (!val.includes(DEFAULT_API_NAME)) throw new Error(`Invalid parameter api_name, it must be an ${DEFAULT_API_NAME}`);
    }
};

export const schema = clonedSchema;

export default class Schema extends ConvictSchema<FileReaderConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const opConfig = getOpConfig(job, 'file_reader') as FileReaderConfig | undefined;
        if (isNil(opConfig)) throw new Error('Could not find opConfig for operation field_reader');
        const { api_name, ...apiConfig } = opConfig;
        if (!Array.isArray(job.apis)) job.apis = [];
        const FileReaderAPI = job.apis.find((jobApi) => jobApi._name === api_name);

        if (isNil(FileReaderAPI)) {
            if (isNil(opConfig.path)) throw new Error(`Invalid parameter path, must be of type string, got ${getTypeOf(opConfig.path)}`);

            job.apis.push({
                _name: DEFAULT_API_NAME,
                ...apiConfig
            });
        } else {
            if (isNotNil(opConfig.path)) throw new Error('If api is specified on this operation, the parameter path must not be specified in the opConfig');
            if (!isEmpty(opConfig.extra_args)) throw new Error('If api is specified on this operation, the parameter extra_args must not be specified in the opConfig');
        }
    }

    build(): Record<string, any> {
        return schema;
    }
}
