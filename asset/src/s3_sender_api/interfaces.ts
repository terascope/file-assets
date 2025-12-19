import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { ChunkedFileSenderAPIConfig, S3Sender } from '@terascope/file-asset-apis';

export interface S3ExporterAPIConfig extends ChunkedFileSenderAPIConfig, APIConfig {}
export type S3SenderFactoryAPI = APIFactoryRegistry<S3Sender, S3ExporterAPIConfig>;
