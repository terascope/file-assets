import { OpConfig } from '@terascope/job-components';
import { ReaderFileConfig } from '../__lib/common-schema';

export interface S3ReaderConfig extends ReaderFileConfig, OpConfig {
}
