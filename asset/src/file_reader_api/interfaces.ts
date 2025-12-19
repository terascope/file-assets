import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { FileTerasliceAPI, ReaderConfig } from '@terascope/file-asset-apis';

export interface FileReaderAPIConfig extends ReaderConfig, APIConfig {}

export type FileReaderFactoryAPI = APIFactoryRegistry<FileTerasliceAPI, FileReaderAPIConfig>;
