import type { RouteSenderAPI } from '@terascope/job-components';
import { Logger, TSError } from '@terascope/utils';
import fse from 'fs-extra';
import { ChunkedFileSender, SendBatchConfig } from '../base';
import { ChunkedFileSenderConfig, FileSenderType } from '../interfaces';

export class FileSender extends ChunkedFileSender implements RouteSenderAPI {
    logger: Logger;

    constructor(config: ChunkedFileSenderConfig, logger: Logger) {
        super(FileSenderType.file, config);
        this.logger = logger;
    }
    /**
     * This is a low level API, it is not meant to be used externally,
     * please use the "send" method instead
     */
    protected async sendToDestination(
        { dest, chunkGenerator }: SendBatchConfig
    ): Promise<void> {
        let output: Buffer|undefined;

        for await (const chunk of chunkGenerator) {
            if (chunk.has_more) {
                throw new Error('has_more is not supported');
            }
            output = chunk.data;
        }

        if (!output) return;
        // Doesn't return a DataEntity or anything else if successful
        try {
            return fse.appendFile(dest, output);
        } catch (err) {
            throw new TSError(err, {
                reason: `Failure to append to file ${dest}`
            });
        }
    }

    /**
     * This method makes sure a directory exists, will throw if it does not exist
     *
     * @example
     *   fileSender.verify('some/path') => Promise<void>
     *   fileSender.verify('some/path/that/does/not/exist) => Error
    */
    async verify(route?: string): Promise<void> {
        const newPath = this.joinPath(route);
        await fse.ensureDir(newPath);
    }
}
