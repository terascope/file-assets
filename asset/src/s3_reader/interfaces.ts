import { OpConfig } from '@terascope/job-components';
import { ReaderFileConfig } from '@terascope/file-asset-apis';

export interface S3ReaderConfig extends ReaderFileConfig, OpConfig {}
