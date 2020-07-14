import { ConvictSchema, cloneDeep } from '@terascope/job-components';
import { HDFSExporterAPIConfig } from './interfaces';
import { schema } from '../s3_exporter/schema';

const { api_name, ...newSchema } = schema;

const apiSchema = cloneDeep(newSchema);
apiSchema.path.format = 'required_String';

export default class Schema extends ConvictSchema<HDFSExporterAPIConfig> {
    build(): Record<string, any> {
        return apiSchema;
    }
}
