import { OpConfig } from '@terascope/job-components';
import { ReaderFileConfig } from '../__lib/interfaces';

export interface HDFSReaderConfig extends ReaderFileConfig, OpConfig {
    user: string;
}
