import { ConvictSchema, cloneDeep } from '@terascope/job-components';
import { FileSenderAPIConfig } from './interfaces.js';
import { commonSchema } from '../__lib/common-schema.js';

const apiSchema = cloneDeep(commonSchema);
apiSchema.path.format = 'required_String';

export default class Schema extends ConvictSchema<FileSenderAPIConfig> {
    build(): Record<string, any> {
        return apiSchema;
    }
}
