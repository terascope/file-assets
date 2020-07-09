import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { ReaderFileConfig } from '../__lib/common-schema';
import FileSender from './sender';

export const DEFAULT_API_NAME = 'file_sender_api';

export interface ReaderFileAPI extends ReaderFileConfig, APIConfig {
    workerId: string;
}

export type FileSenderFactoryAPI = APIFactoryRegistry<FileSender, ReaderFileAPI>
