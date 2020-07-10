import { ConvictSchema, cloneDeep } from '@terascope/job-components';
import { schema } from '../s3_reader/schema';
import { S3ReaderApi } from './interfaces';

const { api_name, ...newSchema } = schema;

const apiSchema = cloneDeep(newSchema);
apiSchema.path.format = 'required_String';

export default class Schema extends ConvictSchema<S3ReaderApi> {
    build(): Record<string, any> {
        return apiSchema;
    }
}
