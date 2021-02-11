import { OpConfig } from '@terascope/job-components';
import { HDFSExportConfig } from '@terascope/file-asset-apis';

export interface HDFSExportOpConfig extends HDFSExportConfig, OpConfig {}
