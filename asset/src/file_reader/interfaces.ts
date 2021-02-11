import { OpConfig } from '@terascope/job-components';
import { ReaderFileConfig } from '@terascope/file-asset-apis';

export interface FileReaderConfig extends ReaderFileConfig, OpConfig {
    api_name: string;
}
