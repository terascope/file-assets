import { OpConfig } from '@terascope/job-components';
import { ReaderFileConfig } from '../__lib/interfaces';

export interface HDFSExportConfig extends ReaderFileConfig, OpConfig {}
