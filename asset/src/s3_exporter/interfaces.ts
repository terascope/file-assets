import { OpConfig } from '@terascope/job-components';
import { ReaderFileConfig } from '../__lib/common-schema';

export interface S3ExportConfig extends ReaderFileConfig, OpConfig {
}

export interface S3PutConfig {
    Bucket: string;
    Key: string;
    Body: string;
}
