import {
    ConvictSchema,
    cloneDeep,
    AnyObject,
    isString,
    getTypeOf,
    ValidatedJobConfig,
    isNil,
    getOpConfig,
    isNotNil,
} from '@terascope/job-components';
import { S3ExportConfig } from './interfaces';
import { fileReaderSchema } from '../__lib/common-schema';
import { DEFAULT_API_NAME } from '../s3_sender_api/interfaces';

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

export default class Schema extends ConvictSchema<S3ExportConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const opConfig = getOpConfig(job, 's3_exporter') as S3ExportConfig | undefined;
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
        } else if (isNotNil(opConfig.path)) throw new Error('If api is specified on this operation, the parameter path must not be specified in the opConfig');
    }

    build(): Record<string, any> {
        return clonedSchema;
    }
}
