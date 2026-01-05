import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { FileTerasliceAPI, ReaderAPIConfig } from '@terascope/file-asset-apis';

export const DEFAULT_API_NAME = 'file_reader_api';

export interface FileReaderAPIConfig extends ReaderAPIConfig, APIConfig {}

export type FileReaderFactoryAPI = APIFactoryRegistry<FileTerasliceAPI, FileReaderAPIConfig>;
