import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { ReaderFileConfig } from '../__lib/common-schema';
import FileSender from './sender';

export const DEFAULT_API_NAME = 's3_sender_api';

export interface S3ExporterAPI extends ReaderFileConfig, APIConfig {
    workerId: string;
}

export type S3SenderFactoryAPI = APIFactoryRegistry<FileSender, S3ExporterAPI>
