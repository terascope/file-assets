import { ConvictSchema } from '@terascope/job-components';
import { S3ExportConfig } from './interfaces';
import { fileReaderSchema } from '../__lib/common-schema';

export default class Schema extends ConvictSchema<S3ExportConfig> {
    build() {
        return fileReaderSchema;
    }
}
