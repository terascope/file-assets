import {
    ConvictSchema,
    cloneDeep,
    AnyObject,
    isString,
    getTypeOf
} from '@terascope/job-components';
import { HDFSReaderApiConfig, DEFAULT_API_NAME } from './interfaces.js';
import { fileReaderSchema } from '../__lib/common-schema.js';

const clonedSchema = cloneDeep(fileReaderSchema) as AnyObject;

clonedSchema.user = {
    doc: 'User to use when reading the files. Default: "hdfs"',
    default: 'hdfs',
    format: 'optional_String'
};

clonedSchema.api_name = {
    doc: 'name of api to be used by hdfs_reader',
    default: DEFAULT_API_NAME,
    format: (val: unknown): void => {
        if (!isString(val)) throw new Error(`Invalid parameter api_name, it must be of type string, was given ${getTypeOf(val)}`);
        if (!val.includes(DEFAULT_API_NAME)) throw new Error(`Invalid parameter api_name, it must be an ${DEFAULT_API_NAME}`);
    }
};
export default class Schema extends ConvictSchema<HDFSReaderApiConfig> {
    build(): Record<string, any> {
        return clonedSchema;
    }
}
