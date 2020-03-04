import {
    BatchProcessor, getClient, ExecutionConfig, WorkerContext, DataEntity
} from '@terascope/job-components';
import { isEmpty } from '@terascope/utils';
import { S3ExportConfig } from './interfaces';
import { parseForFile, makeCsvOptions, CsvOptions } from '../__lib/parser';
import { batchSlice } from '../__lib/slice';
import { getName, parsePath, NameOptions } from '../__lib/fileName';

export default class S3Batcher extends BatchProcessor<S3ExportConfig> {
    client: any;
    sliceCount = -1;
    workerId: string;
    csvOptions: CsvOptions;
    nameOptions: NameOptions;

    constructor(context: WorkerContext, opConfig: S3ExportConfig, exConfig: ExecutionConfig) {
        super(context, opConfig, exConfig);
        this.client = getClient(context, opConfig, 's3');
        this.workerId = context.cluster.worker.id;
        this.csvOptions = makeCsvOptions(opConfig);
        const extension = isEmpty(opConfig.extension) ? undefined : opConfig.extension;

        this.nameOptions = {
            filePath: opConfig.path,
            extension,
            filePerSlice: opConfig.file_per_slice
        };

        // This will be incremented as the worker processes slices and used as a way to create
        // unique object names. Set to -1 so it can be incremented before any slice processing is
        // done
        this.sliceCount = -1;
        // Allows this to use the externalized name builder
    }

    async searchS3(filename: string, list: DataEntity[]) {
        const objPath = parsePath(filename);
        const objName = getName(
            this.workerId,
            this.sliceCount,
            this.nameOptions,
            objPath.prefix
        );
        const outStr = await parseForFile(list, this.opConfig, this.csvOptions);
        // This will prevent empty objects from being added to the S3 store, which can cause
        // problems with the S3 reader
        if (!outStr || outStr.length === 0) {
            return [];
        }

        const params = {
            Bucket: objPath.bucket,
            Key: objName,
            Body: outStr
        };

        return this.client.putObject_Async(params);
    }

    async onBatch(slice: DataEntity[]) {
        // TODO also need to chunk the batches for multipart uploads
        const batches = batchSlice(slice, this.opConfig.path);

        // Needs to be incremented before slice processing so it increments consistently for a given
        // directory
        this.sliceCount += 1;

        const actions = [];

        for (const [filename, list] of Object.entries(batches)) {
            actions.push(this.searchS3(filename, list));
        }

        await Promise.all(actions);

        return slice;
    }
}
