import { ConvictSchema } from '@terascope/job-components';
import { FileConfig } from './interfaces';
import { fileReaderSchema } from '../__lib/common-schema';

export default class Schema extends ConvictSchema<FileConfig> {
    build() {
        return fileReaderSchema;
    }
}
