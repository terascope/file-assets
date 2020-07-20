import {
    RouteSenderAPI,
    DataEntity,
    Logger,
    TSError,
    pMap
} from '@terascope/job-components';
import fse from 'fs-extra';
import ChunkedSender from '../__lib/chunked-file-sender';
import { FileSenderAPIConfig } from './interfaces';
import { FileSenderType } from '../__lib/interfaces';

export default class FileSender extends ChunkedSender implements RouteSenderAPI {
    logger: Logger;
    concurrency: number;

    constructor(config: FileSenderAPIConfig, logger: Logger) {
        super(FileSenderType.file, config as any);
        this.logger = logger;
        const { concurrency } = config;
        this.concurrency = concurrency;
    }

    private async sendToFile(path: string, records: DataEntity[]): Promise<any> {
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

    async send(records: DataEntity[]): Promise<void> {
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
            ([fileName, list]) => this.sendToFile(fileName, list),
            { concurrency }
        );
    }

    async verify(route?: string): Promise<void> {
        const newPath = this.joinPath(route);
        await fse.ensureDir(newPath);
    }
}
