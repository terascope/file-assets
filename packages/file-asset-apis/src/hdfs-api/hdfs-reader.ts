import { AnyObject, Logger } from '@terascope/job-components';
import { ChunkedFileReader } from '../base';
import { FileSlice, ChunkedConfig } from '../interfaces';

export class HDFSReader extends ChunkedFileReader {
    client: AnyObject

    constructor(client: AnyObject, config: ChunkedConfig, logger: Logger) {
        super(config, logger);
        this.client = client;
    }

    /**
     * low level api that fetches the unprocessed contents of the file from HDFS,
     * please use the "read" method for correct file and data parsing
     * @example
     *   const slice = { offset: 0, length: 1000, path: 'some/file.txt', total: 1000 };
     *   const results = await hdfsReader.fetch(slice);
     *   results === 'the unprocessed contents of the file here'
    */
    protected async fetch(slice: FileSlice): Promise<string> {
        const { offset, length, path } = slice;
        return this.client.openAsync(path, { offset, length });
    }
}
