import { ConvictSchema, cloneDeep } from '@terascope/job-components';
import { fileReaderSchema } from '../__lib/common-schema';
import { S3ReaderAPIConfig } from './interfaces';

const apiSchema = cloneDeep(fileReaderSchema);

export default class Schema extends ConvictSchema<S3ReaderAPIConfig> {
    build(): Record<string, any> {
        return apiSchema;
    }
}
