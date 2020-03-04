import { Fetcher, getClient } from '@terascope/job-components';
import { HDFSReaderConfig } from './interfaces';
import { getChunk } from '../__lib/chunked-file-reader';
import { decompress } from '../__lib/compression';
import { SlicedFileResults } from '../__lib/slice';

export default class HDFSFetcher extends Fetcher<HDFSReaderConfig> {
    client: any;

    async initialize() {
        await super.initialize();
        this.client = getClient(this.context, this.opConfig, 'hdfs_ha');
    }

    // TODO: decompress returns a string, but it should be a dataentity
    // @ts-ignore
    async fetch(slice: SlicedFileResults) {
        const reader = async (offset: number, length: number) => {
            const opts = {
                offset,
                length
            };
            return this.client.openAsync(slice.path, opts);
        };
        // Passing the slice in as the `metadata`. This will include the path, offset, and length
        const results = await getChunk(reader, slice, this.opConfig, this.logger, slice);

        return decompress(results.Body, this.opConfig.compression);
    }
}
