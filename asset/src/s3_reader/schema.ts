import { BaseSchema } from '@terascope/job-components';
import { S3ReaderConfig } from './interfaces.js';
import { opSchema } from '../__lib/common-schema.js';

export default class Schema extends BaseSchema<S3ReaderConfig> {
    build(): Record<string, any> {
        return opSchema;
    }
}
