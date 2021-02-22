import {
    RouteSenderAPI,
    DataEntity,
    AnyObject,
    Logger,
    TSError
} from '@terascope/job-components';
import path from 'path';
import { ChunkedFileSender } from '../base';
import { FileSenderType, ChunkedSenderConfig } from '../interfaces';

export class HDFSSender extends ChunkedFileSender implements RouteSenderAPI {
    logger: Logger;
    concurrency: number;
    client: AnyObject;

    constructor(client: AnyObject, config: ChunkedSenderConfig, logger: Logger) {
        super(FileSenderType.hdfs, config as any);
        this.logger = logger;
        const { concurrency } = config;
        this.concurrency = concurrency;
        this.client = client;
    }

    /**
     * This is a low level API, it is not meant to be used externally,
     * please use the "send" method instead
     *
     */
    protected async sendToDestination(
        filename: string, list: (DataEntity | Record<string, unknown>)[]
    ): Promise<any[]> {
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
