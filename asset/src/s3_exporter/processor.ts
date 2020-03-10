import {
    BatchProcessor, getClient, ExecutionConfig, WorkerContext, DataEntity
} from '@terascope/job-components';
import { isEmpty, TSError } from '@terascope/utils';
import { S3ExportConfig, S3PutConfig } from './interfaces';
import { parseForFile, makeCsvOptions } from '../__lib/parser';
import { batchSlice } from '../__lib/slice';
import { getName, parsePath } from '../__lib/fileName';
import { NameOptions, CSVOptions } from '../__lib/interfaces';

export default class S3Batcher extends BatchProcessor<S3ExportConfig> {
    client: any;
    sliceCount = -1;
    workerId: string;
    csvOptions: CSVOptions;
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

    async initialize() {
        await super.initialize();
        await this.ensureBucket();
    }

    async ensureBucket() {
        const { path } = this.opConfig;
        const query = { Bucket: path };

        try {
            await this.client.headBucket_Async(query);
        } catch (_err) {
            try {
                await this.client.createBucket_Async({ Bucket: path });
            } catch (err) {
                throw new TSError(err, { reason: `Could not setup bucket ${path}}` });
            }
        }
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

        const params: S3PutConfig = {
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
