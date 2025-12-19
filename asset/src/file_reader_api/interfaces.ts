import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { FileTerasliceAPI, ReaderAPIConfig } from '@terascope/file-asset-apis';

export interface FileReaderAPIConfig extends ReaderAPIConfig, APIConfig {}

export type FileReaderFactoryAPI = APIFactoryRegistry<FileTerasliceAPI, FileReaderAPIConfig>;
