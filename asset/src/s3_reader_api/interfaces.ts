import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { S3TerasliceAPI, ReaderConfig } from '@terascope/file-asset-apis';

export const DEFAULT_API_NAME = 's3_reader_api';

export interface S3ReaderAPIConfig extends ReaderConfig, APIConfig {}

export type S3ReaderFactoryAPI = APIFactoryRegistry<S3TerasliceAPI, S3ReaderAPIConfig>
