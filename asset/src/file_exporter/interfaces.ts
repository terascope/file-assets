import { OpConfig } from '@terascope/job-components';
import { FileConfig } from '../__lib/common-schema';

export interface FileExporterConfig extends FileConfig, OpConfig {
    api_name: string
}
