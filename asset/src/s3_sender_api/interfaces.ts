import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { ReaderFileConfig } from '../__lib/interfaces';
import FileSender from './sender';

export const DEFAULT_API_NAME = 's3_sender_api';

export interface S3ExporterAPIConfig extends ReaderFileConfig, APIConfig {
    workerId: string;
}

export interface S3PutConfig {
    Bucket: string;
    Key: string;
    Body: string;
}

export type S3SenderFactoryAPI = APIFactoryRegistry<FileSender, S3ExporterAPIConfig>
