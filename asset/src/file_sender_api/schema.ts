import { cloneDeep } from '@terascope/core-utils';
import { BaseSchema } from '@terascope/job-components';
import { FileSenderAPIConfig } from './interfaces.js';
import { commonSchema } from '../__lib/common-schema.js';

const apiSchema = cloneDeep(commonSchema);
apiSchema.path.format = 'required_string';

export default class Schema extends BaseSchema<FileSenderAPIConfig> {
    build(): Record<string, any> {
        return apiSchema;
    }
}
