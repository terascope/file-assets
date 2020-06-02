import { ConvictSchema } from '@terascope/job-components';
import { FileExporterConfig } from './interfaces';
import { commonSchema } from '../__lib/common-schema';

export default class Schema extends ConvictSchema<FileExporterConfig> {
    build(): Record<string, any> {
        return commonSchema;
    }
}
