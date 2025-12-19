import { OpConfig } from '@terascope/job-components';
import { ReaderAPIConfig } from '@terascope/file-asset-apis';

export interface S3ReaderConfig extends ReaderAPIConfig, OpConfig {}
