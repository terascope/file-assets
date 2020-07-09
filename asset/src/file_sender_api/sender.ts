import {
    RouteSenderAPI,
    DataEntity,
    AnyObject,
    Logger,
    TSError,
    pMap
} from '@terascope/job-components';
import fse from 'fs-extra';
import ChunkedSender from '../__lib/chunked-file-sender';
import { batchSlice } from '../__lib/slice';
import { FileSenderType } from '../__lib/interfaces';

export default class FileSender extends ChunkedSender implements RouteSenderAPI {
    logger: Logger;
    concurrency: number;
    pathList = new Map<string, boolean>();

    constructor(config: AnyObject, logger: Logger) {
        super(FileSenderType.file, config as any);
        this.logger = logger;
        const { path, concurrency } = config;
        this.concurrency = concurrency;
        this.pathList.set(path, true);
    }

    async sendToFile(path: string, list: DataEntity[]): Promise<any> {
        // we make dir path if route does not exist
        if (!this.pathList.has(path)) {
            await fse.ensureDir(path);
            this.pathList.set(path, true);
        }

        const { fileName, output } = await this.prepareSegment(list, path);
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

    async send(records: DataEntity[]):Promise<void> {
        // TODO also need to chunk the batches for multipart uploads
        const { concurrency } = this;
        const batches = batchSlice(records, this.config.path);

        this.sliceCount += 1;

        if (!this.config.file_per_slice) {
            if (this.sliceCount > 0) this.fileFormatter.csvOptions.header = false;
        }

        const actions: [string, DataEntity[]][] = [];

        for (const [filename, list] of Object.entries(batches)) {
            actions.push([filename, list]);
        }

        await pMap(
            actions,
            ([fileName, list]) => this.sendToFile(fileName, list),
            { concurrency }
        );
    }
    async verify(fileName: string): Promise<void> {
        // TODO: verify if I need to add path to this
        await fse.ensureDir(fileName);
    }
}
