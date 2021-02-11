import { AnyObject, Logger } from '@terascope/job-components';
import ChunkedReader from '../__lib/chunked-file-reader';
import { SlicedFileResults, ChunkedConfig } from '../interfaces';

export default class HDFSReader extends ChunkedReader {
    client: AnyObject

    constructor(client: AnyObject, config: ChunkedConfig, logger: Logger) {
        super(config, logger);
        this.client = client;
    }
    // TODO: this might not be right
    async fetch(slice: SlicedFileResults): Promise<string> {
        const { offset, length, path } = slice;
        return this.client.openAsync(path, { offset, length });
    }
}
