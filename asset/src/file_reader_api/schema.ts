import { cloneDeep } from '@terascope/core-utils';
import { BaseSchema } from '@terascope/job-components';
import { fileReaderSchema } from '../__lib/common-schema.js';
import { FileReaderAPIConfig } from './interfaces.js';

const apiSchema = cloneDeep(fileReaderSchema);

export default class Schema extends BaseSchema<FileReaderAPIConfig> {
    build(): Record<string, any> {
        return apiSchema;
    }
}
