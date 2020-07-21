import { OpConfig } from '@terascope/job-components';
import { ReaderFileConfig } from '../__lib/interfaces';

export interface S3ExportConfig extends ReaderFileConfig, OpConfig {}
