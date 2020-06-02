import { Fetcher, getClient } from '@terascope/job-components';
import { HDFSReaderConfig } from './interfaces';
import { getChunk } from '../__lib/chunked-file-reader';
import { decompress } from '../__lib/compression';
import { SlicedFileResults } from '../__lib/interfaces';

export default class HDFSFetcher extends Fetcher<HDFSReaderConfig> {
    client: any;

    async initialize(): Promise<void> {
        await super.initialize();
        this.client = getClient(this.context, this.opConfig, 'hdfs_ha');
    }

    async getHdfsData(slice: SlicedFileResults): Promise<any> {
        const { offset, length, path } = slice;
        return this.client.openAsync(path, { offset, length });
    }

    // @ts-expect-error
    async fetch(slice: SlicedFileResults): Promise<string> {
        const results = await getChunk(this.getHdfsData, this.opConfig, this.logger, slice);
        // @ts-expect-error TODO: need to verify data from client, and use getChunk fn
        return decompress(results.Body, this.opConfig.compression);
    }
}
