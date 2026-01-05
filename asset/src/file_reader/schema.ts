import { ConvictSchema } from '@terascope/job-components';
import { FileReaderConfig } from './interfaces.js';
import { opSchema } from '../__lib/common-schema.js';

export default class Schema extends ConvictSchema<FileReaderConfig> {
    build(): Record<string, any> {
        return opSchema;
    }
}
