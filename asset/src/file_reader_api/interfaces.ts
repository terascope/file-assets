import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { FileReader, ReaderFileConfig } from '@terascope/file-asset-apis';

export const DEFAULT_API_NAME = 'file_reader_api';

export interface FileReaderAPIConfig extends ReaderFileConfig, APIConfig {}

export type FileReaderFactoryAPI = APIFactoryRegistry<FileReader, FileReaderAPIConfig>
