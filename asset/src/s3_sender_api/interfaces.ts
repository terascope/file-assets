import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { BaseSenderConfig, S3Sender } from '@terascope/file-asset-apis';

export const DEFAULT_API_NAME = 's3_sender_api';
export interface S3ExporterAPIConfig extends BaseSenderConfig, APIConfig {}
export type S3SenderFactoryAPI = APIFactoryRegistry<S3Sender, S3ExporterAPIConfig>
