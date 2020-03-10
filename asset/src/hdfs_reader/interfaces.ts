import { OpConfig } from '@terascope/job-components';
import { ReaderFileConfig } from '../__lib/common-schema';

export interface HDFSReaderConfig extends ReaderFileConfig, OpConfig {
    user: string;
}
