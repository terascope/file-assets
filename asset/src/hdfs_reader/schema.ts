import {
    ConvictSchema,
    ValidatedJobConfig,
    getOpConfig,
    isNil,
    cloneDeep,
    AnyObject,
    isString,
    getTypeOf,
    isEmpty
} from '@terascope/job-components';
import { HDFSReaderConfig } from './interfaces';
import { fileReaderSchema, compareConfig } from '../__lib/common-schema';
import { DEFAULT_API_NAME } from '../hdfs_reader_api/interfaces';

const clonedSchema = cloneDeep(fileReaderSchema) as AnyObject;

clonedSchema.user = {
    doc: 'User to use when reading the files. Default: "hdfs"',
    default: 'hdfs',
    format: 'optional_String'
};

clonedSchema.api_name = {
    doc: 'name of api to be used by elasticearch reader',
    default: DEFAULT_API_NAME,
    format: (val: unknown): void => {
        if (!isString(val)) throw new Error(`Invalid parameter api_name, it must be of type string, was given ${getTypeOf(val)}`);
        if (!val.includes(DEFAULT_API_NAME)) throw new Error(`Invalid parameter api_name, it must be an ${DEFAULT_API_NAME}`);
    }
};

export const schema = clonedSchema;

export default class Schema extends ConvictSchema<HDFSReaderConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const opConfig = getOpConfig(job, 'hdfs_reader') as HDFSReaderConfig | undefined;
        if (isNil(opConfig)) throw new Error('Could not find opConfig for operation hdfs_reader');
        const { api_name, ...apiConfig } = opConfig;
        if (!Array.isArray(job.apis)) job.apis = [];
        const HDFSReaderAPI = job.apis.find((jobApi) => jobApi._name === api_name);

        if (isNil(HDFSReaderAPI)) {
            if (isNil(opConfig.path)) throw new Error(`Invalid parameter path, must be of type string, got ${getTypeOf(opConfig.path)}`);

            job.apis.push({
                _name: DEFAULT_API_NAME,
                ...apiConfig
            });
        } else {
            if (!isEmpty(opConfig.extra_args)) throw new Error('If api is specified on this operation, the parameter extra_args must not be specified in the opConfig');
            compareConfig(opConfig, HDFSReaderAPI);
        }
    }

    build(): Record<string, any> {
        return schema;
    }
}
