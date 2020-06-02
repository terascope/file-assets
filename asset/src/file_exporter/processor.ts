import {
    BatchProcessor, WorkerContext, ExecutionConfig,
    TSError, DataEntity, isEmpty, pMap
} from '@terascope/job-components';
import fse from 'fs-extra';
import { FileExporterConfig } from './interfaces';
import { getName } from '../__lib/fileName';
import { batchSlice } from '../__lib/slice';
import { parseForFile, makeCsvOptions } from '../__lib/parser';
import { NameOptions, CSVOptions } from '../__lib/interfaces';

export default class FileBatcher extends BatchProcessor<FileExporterConfig> {
    workerId: string;
    sliceCount: number;
    firstSlice: boolean;
    concurrency: number;
    csvOptions: CSVOptions;
    nameOptions: NameOptions;
    pathList = new Map<string, boolean>();

    constructor(
        context: WorkerContext, opConfig: FileExporterConfig, executionConfig: ExecutionConfig
    ) {
        super(context, opConfig, executionConfig);
        const extension = isEmpty(opConfig.extension) ? undefined : opConfig.extension;
        const { path, concurrency } = opConfig;
        this.concurrency = concurrency;
        this.nameOptions = {
            filePath: path,
            extension,
            filePerSlice: opConfig.file_per_slice
        };
        this.pathList.set(path, true);
        this.workerId = context.cluster.worker.id;
        // Coerce `file_per_slice` for JSON format or compressed output
        if ((opConfig.format === 'json') || (opConfig.compression !== 'none')) {
            this.nameOptions.filePerSlice = true;
        }
        // Used for incrementing file name with `file_per_slice`
        this.sliceCount = -1;
        this.firstSlice = true;
        // Set the options for the parser
        this.csvOptions = makeCsvOptions(this.opConfig);
    }

    async sendToFile(path: string, list: DataEntity[]): Promise<void> {
        // we make dir path if route does not exist
        if (!this.pathList.has(path)) {
            await fse.ensureDir(path);
            this.pathList.set(path, true);
        }

        const fileName = getName(this.workerId, this.sliceCount, this.nameOptions, path);
        const outStr = await parseForFile(list, this.opConfig, this.csvOptions);

        // Prevents empty slices from resulting in empty files
        if (!outStr || outStr.length === 0) {
            return;
        }

        // Doesn't return a DataEntity or anything else if successful
        try {
            return fse.appendFile(fileName, outStr);
        } catch (err) {
            throw new TSError(err, {
                reason: `Failure to append to file ${fileName}`
            });
        }
    }

    async onBatch(slice: DataEntity[]): Promise<DataEntity[]> {
        // TODO also need to chunk the batches for multipart uploads
        const { concurrency } = this;
        const batches = batchSlice(slice, this.opConfig.path);

        this.sliceCount += 1;

        if (!this.opConfig.file_per_slice) {
            if (this.sliceCount > 0) this.csvOptions.header = false;
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

        return slice;
    }
}
