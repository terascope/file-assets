import { OpConfig } from '@terascope/job-components';
import { FileConfig } from '@terascope/file-asset-apis';

export interface FileExporterConfig extends FileConfig, OpConfig {
    api_name: string
}
