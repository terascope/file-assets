import { ConvictSchema } from '@terascope/job-components';
import { HDFSExporterAPIConfig } from './interfaces';
import { fileReaderSchema } from '../__lib/common-schema';

export default class Schema extends ConvictSchema<HDFSExporterAPIConfig> {
    build(): Record<string, any> {
        return fileReaderSchema;
    }
}
