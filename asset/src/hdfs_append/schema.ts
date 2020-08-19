import {
    ConvictSchema,
    cloneDeep,
    AnyObject,
    isString,
    getTypeOf,
    ValidatedJobConfig,
    isNil,
    getOpConfig
} from '@terascope/job-components';
import { HDFSExportConfig } from './interfaces';
import { DEFAULT_API_NAME } from '../hdfs_sender_api/interfaces';
import { fileReaderSchema, compareConfig } from '../__lib/common-schema';

const clonedSchema = cloneDeep(fileReaderSchema) as AnyObject;

clonedSchema.api_name = {
    doc: 'name of api to be used by hdfs_append',
    default: DEFAULT_API_NAME,
    format: (val: unknown): void => {
        if (!isString(val)) throw new Error(`Invalid parameter api_name, it must be of type string, was given ${getTypeOf(val)}`);
        if (!val.includes(DEFAULT_API_NAME)) throw new Error(`Invalid parameter api_name, it must be an ${DEFAULT_API_NAME}`);
    }
};

export const schema = clonedSchema;

export default class Schema extends ConvictSchema<HDFSExportConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const opConfig = getOpConfig(job, 'hdfs_exporter') as HDFSExportConfig | undefined;
        if (isNil(opConfig)) throw new Error('Could not find opConfig for operation hdfs_exporter');
        const { api_name, ...apiConfig } = opConfig;
        if (!Array.isArray(job.apis)) job.apis = [];
        const HDFSSenderAPI = job.apis.find((jobApi) => jobApi._name === api_name);

        if (isNil(HDFSSenderAPI)) {
            if (isNil(opConfig.path)) throw new Error(`Invalid parameter path, must be of type string, got ${getTypeOf(opConfig.path)}`);

            job.apis.push({
                _name: DEFAULT_API_NAME,
                ...apiConfig
            });
        } else {
            compareConfig(opConfig, HDFSSenderAPI);
        }
    }

    build(): Record<string, any> {
        return fileReaderSchema;
    }
}
