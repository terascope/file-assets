import { ConvictSchema } from '@terascope/job-components';
import { S3ExporterAPIConfig } from './interfaces';
import { fileReaderSchema } from '../__lib/common-schema';

export default class Schema extends ConvictSchema<S3ExporterAPIConfig> {
    build(): Record<string, any> {
        return fileReaderSchema;
    }
}
