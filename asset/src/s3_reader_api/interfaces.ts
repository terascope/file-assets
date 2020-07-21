import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import S3Reader from './reader';
import { ReaderFileConfig } from '../__lib/interfaces';

export const DEFAULT_API_NAME = 's3_reader_api';

export interface S3ReaderAPIConfig extends ReaderFileConfig, APIConfig {}

export type S3ReaderFactoryAPI = APIFactoryRegistry<S3Reader, S3ReaderAPIConfig>
