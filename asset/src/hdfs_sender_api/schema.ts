import { ConvictSchema } from '@terascope/job-components';
import { HDFSExporterAPIConfig } from './interfaces.js';
import { fileReaderSchema } from '../__lib/common-schema.js';

export default class Schema extends ConvictSchema<HDFSExporterAPIConfig> {
    build(): Record<string, any> {
        return fileReaderSchema;
    }
}
