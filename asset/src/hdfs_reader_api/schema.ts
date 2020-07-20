import { ConvictSchema, cloneDeep } from '@terascope/job-components';
import { schema } from '../hdfs_reader/schema';
import { HDFSReaderApiConfig } from './interfaces';

const { api_name, ...newSchema } = schema;

const apiSchema = cloneDeep(newSchema);
apiSchema.path.format = 'required_String';

export default class Schema extends ConvictSchema<HDFSReaderApiConfig> {
    build(): Record<string, any> {
        return apiSchema;
    }
}