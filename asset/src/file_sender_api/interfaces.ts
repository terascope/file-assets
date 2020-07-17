import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { ReaderFileConfig } from '../__lib/interfaces';
import FileSender from './sender';

export const DEFAULT_API_NAME = 'file_sender_api';

export interface FileSenderAPIConfig extends ReaderFileConfig, APIConfig {
    workerId: string;
}

export type FileSenderFactoryAPI = APIFactoryRegistry<FileSender, FileSenderAPIConfig>
