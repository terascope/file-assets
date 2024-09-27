import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { FileSender, ChunkedFileSenderConfig } from '@terascope/file-asset-apis';

export const DEFAULT_API_NAME = 'file_sender_api';

export interface FileSenderAPIConfig extends ChunkedFileSenderConfig, APIConfig {}

export type FileSenderFactoryAPI = APIFactoryRegistry<FileSender, FileSenderAPIConfig>;
