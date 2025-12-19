import { OpConfig } from '@terascope/job-components';
import { ReaderAPIConfig } from '@terascope/file-asset-apis';

export interface FileReaderConfig extends ReaderAPIConfig, OpConfig {
    _api_name: string;
}
