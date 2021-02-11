import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { S3ExporterConfig, S3Sender } from '@terascope/file-asset-apis';

export const DEFAULT_API_NAME = 's3_sender_api';
export interface S3ExporterAPIConfig extends S3ExporterConfig, APIConfig {}
export type S3SenderFactoryAPI = APIFactoryRegistry<S3Sender, S3ExporterAPIConfig>
