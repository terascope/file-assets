import {
    RouteSenderAPI,
    DataEntity,
    AnyObject,
    Logger,
    TSError,
    pMap
} from '@terascope/job-components';
import path from 'path';
import ChunkedSender from '../__lib/chunked-file-sender';
import { FileSenderType, HDFSExportConfig } from '../interfaces';

export default class HDFSSender extends ChunkedSender implements RouteSenderAPI {
    logger: Logger;
    concurrency: number;
    client: AnyObject;

    constructor(client: AnyObject, config: HDFSExportConfig, logger: Logger) {
        super(FileSenderType.hdfs, config as any);
        this.logger = logger;
        const { concurrency } = config;
        this.concurrency = concurrency;
        this.client = client;
    }

    async sendToHdfs(filename: string, list: DataEntity[]): Promise<any[]> {
        const { fileName, output } = await this.prepareSegment(filename, list);

        // This will prevent empty objects from being added to the S3 store, which can cause
        // problems with the S3 reader
        if (!output || output.length === 0) {
            return [];
        }

        try {
            return this.client.appendAsync(fileName, output);
        } catch (err) {
            throw new TSError(err, {
                reason: 'Error sending data to file',
                context: {
                    file: fileName
                }
            });
        }
    }
    async send(records: DataEntity[]):Promise<void> {
        const { concurrency } = this;
        this.sliceCount += 1;

        if (!this.config.file_per_slice) {
            if (this.sliceCount > 0) this.fileFormatter.csvOptions.header = false;
        }

        const dispatch = this.prepareDispatch(records);

        const actions: [string, DataEntity[]][] = [];

        for (const [filename, list] of Object.entries(dispatch)) {
            actions.push([filename, list]);
        }

        await pMap(
            actions,
            ([fileName, list]) => this.sendToHdfs(fileName, list),
            { concurrency }
        );
    }

    async verify(route: string): Promise<void> {
        const newPath = this.joinPath(route);

        try {
            return this.client.getFileStatusAsync(newPath);
        } catch (_err) {
            try {
                await this.client.mkdirsAsync(path.dirname(newPath));
                await this.client.createAsync(newPath, '');
            } catch (err) {
                new TSError(err, {
                    reason: 'Error while attempting to create a file',
                    context: {
                        newPath
                    }
                });
            }
        }
    }
}
