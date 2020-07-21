import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { HDFSExportConfig } from '../hdfs_append/interfaces';
import HDFSSender from './sender';

export const DEFAULT_API_NAME = 'hdfs_sender_api';

// TODO: verify workerID
export interface HDFSExporterAPIConfig extends HDFSExportConfig, APIConfig {
    workerId: string;
}

export type HDFSSenderFactoryAPI = APIFactoryRegistry<HDFSSender, HDFSExporterAPIConfig>
