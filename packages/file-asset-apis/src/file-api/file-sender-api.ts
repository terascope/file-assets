import {
    RouteSenderAPI,
    DataEntity,
    Logger,
    TSError,
    pMap
} from '@terascope/job-components';
import fse from 'fs-extra';
import { ChunkedFileSender } from '../lib';
import { ChunkedSenderConfig, FileSenderType } from '../interfaces';

export class FileSender extends ChunkedFileSender implements RouteSenderAPI {
    logger: Logger;
    concurrency: number;

    constructor(config: ChunkedSenderConfig, logger: Logger) {
        super(FileSenderType.file, config);
        this.logger = logger;
        const { concurrency } = config;
        this.concurrency = concurrency;
    }

    private async sendToFile(
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
     * Write data to file
     *
     * @example
     * fileSender.send([{ some: 'data' }]) => Promise<void>
     * fileSender.send([DataEntity.make({ some: 'data' })]) => Promise<void>
    */
    async send(records: (DataEntity | Record<string, unknown>)[]): Promise<void> {
        const { concurrency } = this;
        this.sliceCount += 1;

        if (!this.config.file_per_slice) {
            if (this.sliceCount > 0) this.fileFormatter.csvOptions.header = false;
        }

        const dispatch = this.prepareDispatch(records);

        const actions: [string, (DataEntity | Record<string, unknown>)[]][] = [];

        for (const [filename, list] of Object.entries(dispatch)) {
            actions.push([filename, list]);
        }

        await pMap(
            actions,
            ([fileName, list]) => this.sendToFile(fileName, list),
            { concurrency }
        );
    }

    /**
     * This method makes sure a directory exists, will throw if it does not exist
     *
     * @example
     * fileSender.verify('some/path') => Promise<void>
     * fileSender.verify('some/path/that/does/not/exist) => Error
    */
    async verify(route?: string): Promise<void> {
        const newPath = this.joinPath(route);
        await fse.ensureDir(newPath);
    }
}
