import { ConvictSchema, cloneDeep } from '@terascope/job-components';
import { schema } from '../file_exporter/schema';
import { ReaderFileAPI } from './interfaces';

const { api_name, ...newSchema } = schema;

const apiSchema = cloneDeep(newSchema);
apiSchema.path.format = 'required_String';

export default class Schema extends ConvictSchema<ReaderFileAPI> {
    build(): Record<string, any> {
        return apiSchema;
    }
}
