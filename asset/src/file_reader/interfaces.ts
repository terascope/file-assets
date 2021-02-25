import { OpConfig } from '@terascope/job-components';
import { ReaderConfig } from '@terascope/file-asset-apis';

export interface FileReaderConfig extends ReaderConfig, OpConfig {
    api_name: string;
}
