import { OpConfig } from '@terascope/job-components';
import { ChunkedFileSenderConfig } from '@terascope/file-asset-apis';

export interface FileExporterConfig extends ChunkedFileSenderConfig, OpConfig {
    api_name: string
}
