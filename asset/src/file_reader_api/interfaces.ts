import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import FileReader from './reader';
import { ReaderFileConfig } from '../__lib/interfaces';

export const DEFAULT_API_NAME = 'file_reader_api';

export interface FileReaderAPIConfig extends ReaderFileConfig, APIConfig {}

export type FileReaderFactoryAPI = APIFactoryRegistry<FileReader, FileReaderAPIConfig>
