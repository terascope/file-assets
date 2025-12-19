import { OpConfig } from '@terascope/job-components';
import { ChunkedFileSenderAPIConfig } from '@terascope/file-asset-apis';

export interface FileExporterConfig extends ChunkedFileSenderAPIConfig, OpConfig {
    _api_name: string;
}
