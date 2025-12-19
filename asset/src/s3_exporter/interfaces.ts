import { OpConfig } from '@terascope/job-components';
import { ChunkedFileSenderAPIConfig } from '@terascope/file-asset-apis';

export interface S3ExportConfig extends ChunkedFileSenderAPIConfig, OpConfig {}
