import { OpConfig } from '@terascope/job-components';
import { ChunkedFileSenderConfig } from '@terascope/file-asset-apis';

export interface HDFSExportOpConfig extends ChunkedFileSenderConfig, OpConfig {}
