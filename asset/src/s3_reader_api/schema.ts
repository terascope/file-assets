import { cloneDeep } from '@terascope/core-utils';
import { ConvictSchema } from '@terascope/job-components';
import { fileReaderSchema } from '../__lib/common-schema.js';
import { S3ReaderAPIConfig } from './interfaces.js';

const apiSchema = cloneDeep(fileReaderSchema);

export default class Schema extends ConvictSchema<S3ReaderAPIConfig> {
    build(): Record<string, any> {
        return apiSchema;
    }
}
