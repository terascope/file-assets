import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { FileSender, ChunkedFileSenderAPIConfig } from '@terascope/file-asset-apis';

export interface FileSenderAPIConfig extends ChunkedFileSenderAPIConfig, APIConfig {}

export type FileSenderFactoryAPI = APIFactoryRegistry<FileSender, FileSenderAPIConfig>;
