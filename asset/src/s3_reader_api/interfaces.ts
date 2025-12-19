import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { S3TerasliceAPI, ReaderAPIConfig } from '@terascope/file-asset-apis';

export interface S3ReaderAPIConfig extends ReaderAPIConfig, APIConfig {}

export type S3ReaderFactoryAPI = APIFactoryRegistry<S3TerasliceAPI, S3ReaderAPIConfig>;
