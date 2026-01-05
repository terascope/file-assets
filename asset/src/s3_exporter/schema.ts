import { ConvictSchema } from '@terascope/job-components';
import { S3ExportConfig } from './interfaces.js';
import { opSchema } from '../__lib/common-schema.js';

export default class Schema extends ConvictSchema<S3ExportConfig> {
    build(): Record<string, any> {
        return opSchema;
    }
}
