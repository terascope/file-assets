import {
    RouteSenderAPI,
    AnyObject,
    Logger,
    TSError
} from '@terascope/utils';
import path from 'node:path';
import { ChunkedFileSender, SendBatchConfig } from '../base/index.js';
import { FileSenderType, ChunkedFileSenderConfig } from '../interfaces.js';

export class HDFSSender extends ChunkedFileSender implements RouteSenderAPI {
    client: AnyObject;

    constructor(client: AnyObject, config: ChunkedFileSenderConfig, logger: Logger) {
        super(FileSenderType.hdfs, config, logger);
        this.client = client;
    }

    /**
     * This is a low level API, it is not meant to be used externally,
     * please use the "send" method instead.
     *
     * @todo THIS PROBABLY NOT WORK and is missing some logic in the file sender
     */
    protected async sendToDestination(
        { dest, chunkGenerator }: SendBatchConfig
    ): Promise<void> {
        let output: Buffer|string|undefined;

        for await (const chunk of chunkGenerator) {
            if (chunk.has_more) {
                throw new Error('has_more is not supported');
            }
            output = chunk.data;
        }

        try {
            return this.client.appendAsync(dest, output);
        } catch (err) {
            throw new TSError(err, {
                reason: 'Error sending data to file',
                context: {
                    file: dest
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
