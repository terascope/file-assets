import { OpConfig } from '@terascope/job-components';
import { ReaderFileConfig } from '../__lib/interfaces';

export interface FileReaderConfig extends ReaderFileConfig, OpConfig {
    api_name: string;
}
