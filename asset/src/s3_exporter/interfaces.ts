import { OpConfig } from '@terascope/job-components';
import { ReaderFileConfig } from '../__lib/common-schema';

export interface S3ExportConfig extends ReaderFileConfig, OpConfig {
}
