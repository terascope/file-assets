import { OpConfig } from '@terascope/job-components';
import { ReaderFileConfig } from '../__lib/common-schema';

export interface HDFSConfig extends ReaderFileConfig, OpConfig {}
