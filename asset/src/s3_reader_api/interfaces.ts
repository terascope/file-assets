import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { S3Reader, ReaderFileConfig } from '@terascope/file-asset-apis';

export const DEFAULT_API_NAME = 's3_reader_api';

export interface S3ReaderAPIConfig extends ReaderFileConfig, APIConfig {}

export type S3ReaderFactoryAPI = APIFactoryRegistry<S3Reader, S3ReaderAPIConfig>
