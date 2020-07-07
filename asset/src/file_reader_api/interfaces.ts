import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import FileReader from './reader';
import { ReaderFileConfig } from '../__lib/common-schema';

export const DEFAULT_API_NAME = 'file_reader_api';

export interface ReaderFileAPI extends ReaderFileConfig, APIConfig {}

export type FileReaderFactoryAPI = APIFactoryRegistry<FileReader, ReaderFileAPI>
