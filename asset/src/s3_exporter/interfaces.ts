import { OpConfig } from '@terascope/job-components';
import { BaseSenderConfig } from '@terascope/file-asset-apis';

export interface S3ExportConfig extends BaseSenderConfig, OpConfig {}
