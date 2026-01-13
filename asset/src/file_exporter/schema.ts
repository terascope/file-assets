import { BaseSchema } from '@terascope/job-components';
import { FileExporterConfig } from './interfaces.js';
import { opSchema } from '../__lib/common-schema.js';

export default class Schema extends BaseSchema<FileExporterConfig> {
    build(): Record<string, any> {
        return opSchema;
    }
}
