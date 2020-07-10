import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import S3Reader from './reader';
import { ReaderFileConfig } from '../__lib/common-schema';

export const DEFAULT_API_NAME = 's3_reader_api';

export interface S3ReaderApi extends ReaderFileConfig, APIConfig {}

export type S3ReaderFactoryAPI = APIFactoryRegistry<S3Reader, S3ReaderApi>
