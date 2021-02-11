import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { HDFSExportConfig, HDFSSender } from '@terascope/file-asset-apis';

export const DEFAULT_API_NAME = 'hdfs_sender_api';

export interface HDFSExporterAPIConfig extends HDFSExportConfig, APIConfig {
    workerId: string;
}

export type HDFSSenderFactoryAPI = APIFactoryRegistry<HDFSSender, HDFSExporterAPIConfig>
