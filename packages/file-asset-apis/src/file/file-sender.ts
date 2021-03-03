import type { RouteSenderAPI } from '@terascope/job-components';
import { Logger } from '@terascope/utils';
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
        let fd:number|undefined;

        try {
            for await (const chunk of chunkGenerator) {
                // we can't create the file descriptor unless
                // there are chunks, since calling open will create
                // the file for empty slices
                if (fd == null) {
                    fd = await fse.open(dest, 'a');
                }
                await fse.appendFile(fd, chunk.data);
            }
        } finally {
            if (fd != null) await fse.close(fd);
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
