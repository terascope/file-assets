import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { HDFSReader, HDFSReaderConfig } from '@terascope/file-asset-apis';

export const DEFAULT_API_NAME = 'hdfs_reader_api';

export interface HDFSReaderApiConfig extends HDFSReaderConfig, APIConfig {}

export type HDFSReaderFactoryAPI = APIFactoryRegistry<HDFSReader, HDFSReaderApiConfig>;
