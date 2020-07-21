import { OpConfig } from '@terascope/job-components';
import { ReaderFileConfig } from '../__lib/interfaces';

export interface S3ReaderConfig extends ReaderFileConfig, OpConfig {}
