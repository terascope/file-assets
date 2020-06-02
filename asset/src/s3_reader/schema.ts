import { ConvictSchema } from '@terascope/job-components';
import { S3ReaderConfig } from './interfaces';
import { fileReaderSchema } from '../__lib/common-schema';

export default class Schema extends ConvictSchema<S3ReaderConfig> {
    build(): Record<string, any> {
        return fileReaderSchema;
    }
}
