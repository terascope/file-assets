import { OpConfig } from '@terascope/job-components';
import { BaseSenderConfig } from '@terascope/file-asset-apis';

export interface FileExporterConfig extends BaseSenderConfig, OpConfig {
    api_name: string
}
