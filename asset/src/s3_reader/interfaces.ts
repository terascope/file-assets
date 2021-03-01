import { OpConfig } from '@terascope/job-components';
import { ReaderConfig } from '@terascope/file-asset-apis';

export interface S3ReaderConfig extends ReaderConfig, OpConfig {}
