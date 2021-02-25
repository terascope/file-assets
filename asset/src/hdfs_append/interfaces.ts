import { OpConfig } from '@terascope/job-components';
import { BaseSenderConfig } from '@terascope/file-asset-apis';

export interface HDFSExportOpConfig extends BaseSenderConfig, OpConfig {}
