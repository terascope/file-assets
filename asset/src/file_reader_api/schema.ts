import { ConvictSchema, cloneDeep } from '@terascope/job-components';
import { fileReaderSchema } from '../__lib/common-schema.js';
import { FileReaderAPIConfig } from './interfaces.js';

const apiSchema = cloneDeep(fileReaderSchema);

export default class Schema extends ConvictSchema<FileReaderAPIConfig> {
    build(): Record<string, any> {
        return apiSchema;
    }
}
