import type { RouteSenderAPI } from '@terascope/job-components';
import {
    DataEntity,
    Logger,
    TSError
} from '@terascope/utils';
import fse from 'fs-extra';
import { ChunkedFileSender } from '../base';
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
        path: string, records: (DataEntity | Record<string, unknown>)[]
    ): Promise<any> {
        const { fileName, output } = await this.prepareSegment(path, records);
        // Prevents empty slices from resulting in empty files
        if (!output || output.length === 0) {
            return [];
        }

        // Doesn't return a DataEntity or anything else if successful
        try {
            return fse.appendFile(fileName, output);
        } catch (err) {
            throw new TSError(err, {
                reason: `Failure to append to file ${fileName}`
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
