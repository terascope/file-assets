import { OpConfig } from '@terascope/job-components';
import { ChunkedFileSenderConfig } from '@terascope/file-asset-apis';

export interface FileExporterConfig extends ChunkedFileSenderConfig, OpConfig {
    _api_name: string;
}
