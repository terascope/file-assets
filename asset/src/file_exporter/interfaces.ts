import { OpConfig } from '@terascope/job-components';
import { FileConfig } from '../__lib/interfaces';

export interface FileExporterConfig extends FileConfig, OpConfig {
    api_name: string
}
