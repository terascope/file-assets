import { APIConfig, APIFactoryRegistry } from '@terascope/job-components';
import { ChunkedSenderConfig, HDFSSender } from '@terascope/file-asset-apis';

export const DEFAULT_API_NAME = 'hdfs_sender_api';

export interface HDFSExporterAPIConfig extends ChunkedSenderConfig, APIConfig {}

export type HDFSSenderFactoryAPI = APIFactoryRegistry<HDFSSender, HDFSExporterAPIConfig>
