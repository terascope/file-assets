import { OpConfig } from '@terascope/job-components';
import { HDFSReaderConfig } from '@terascope/file-asset-apis';

export interface HDFSReaderOpConfig extends HDFSReaderConfig, OpConfig {}
