import { ConvictSchema } from '@terascope/job-components';
import { HDFSConfig } from './interfaces';
import { fileReaderSchema } from '../__lib/common-schema';

export default class Schema extends ConvictSchema<HDFSConfig> {
    build(): Record<string, any> {
        return fileReaderSchema;
    }
}
