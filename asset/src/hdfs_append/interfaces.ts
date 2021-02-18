import { OpConfig } from '@terascope/job-components';
import { ChunkedSenderConfig } from '@terascope/file-asset-apis';

export interface HDFSExportOpConfig extends ChunkedSenderConfig, OpConfig {}
