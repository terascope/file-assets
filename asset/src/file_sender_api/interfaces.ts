import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { FileSender, ChunkedFileSenderConfig } from '@terascope/file-asset-apis';

export interface FileSenderAPIConfig extends ChunkedFileSenderConfig, APIConfig {}

export type FileSenderFactoryAPI = APIFactoryRegistry<FileSender, FileSenderAPIConfig>;
