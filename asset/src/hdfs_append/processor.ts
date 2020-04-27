import {
    BatchProcessor, getClient, WorkerContext, ExecutionConfig, DataEntity, GetClientConfig,
    TSError, isEmpty
} from '@terascope/job-components';
import path from 'path';
import { HDFSConfig } from './interfaces';
import { parseForFile, makeCsvOptions } from '../__lib/parser';
import { batchSlice } from '../__lib/slice';
import { getName } from '../__lib/fileName';
import { NameOptions, CSVOptions } from '../__lib/interfaces';

export default class HDFSBatcher extends BatchProcessor<HDFSConfig> {
    sliceCount: number;
    client: any;
    workerId: string;
    csvOptions: CSVOptions;
    nameOptions: NameOptions;

    constructor(context: WorkerContext, opConfig: HDFSConfig, executionConfig: ExecutionConfig) {
        super(context, opConfig, executionConfig);
        // Client connection cannot be cached, an endpoint needs to be re-instantiated for a
        // different namenode_host
        const { connection } = opConfig;
        const clientConfig: GetClientConfig = {
            connection_cache: false,
            connection
        };
        opConfig.connection_cache = false;
        const extension = isEmpty(opConfig.extension) ? undefined : opConfig.extension;

        this.client = getClient(context, clientConfig, 'hdfs_ha').client;
        this.workerId = context.cluster.worker.id;
        this.nameOptions = {
            filePath: opConfig.path,
            extension,
            filePerSlice: opConfig.file_per_slice
        };

        // This will be incremented as the worker processes slices and used as a way to create
        // unique object names. Set to -1 so it can be incremented before any slice processing is
        // done
        this.sliceCount = -1;
        this.csvOptions = makeCsvOptions(opConfig);

        // The append error detection and name change system need to be reworked to be compatible
        // with the file batching. In the meantime, restarting the job will sidestep the issue with
        // new worker names.
        // this.appendErrors = {};
    }

    async ensureFile(fileName: string) {
        try {
            return this.client.getFileStatusAsync(fileName);
        } catch (_err) {
            try {
                await this.client.mkdirsAsync(path.dirname(fileName));
                await this.client.createAsync(fileName, '');
            } catch (err) {
                new TSError(err, {
                    reason: 'Error while attempting to create a file',
                    context: {
                        fileName
                    }
                });
            }
        }
    }

    async searchHdfs(filename: string, list: DataEntity[]) {
        const fileName = getName(this.workerId, this.sliceCount, this.nameOptions, filename);
        const outStr = await parseForFile(list, this.opConfig, this.csvOptions);

        // This will prevent empty objects from being added to the S3 store, which can cause
        // problems with the S3 reader
        if (!outStr || outStr.length === 0) {
            return [];
        }

        await this.ensureFile(fileName);

        try {
            return this.client.appendAsync(fileName, outStr);
        } catch (err) {
            throw new TSError(err, {
                reason: 'Error sending data to file',
                context: {
                    file: fileName
                }
            });
        }
    }

    async onBatch(slice: DataEntity[]) {
        // TODO also need to chunk the batches for multipart uploads
        const batches = batchSlice(slice, this.opConfig.path);

        // Needs to be incremented before slice processing so it increments consistently for a given
        // directory
        this.sliceCount += 1;

        const actions = [];

        for (const [filename, list] of Object.entries(batches)) {
            actions.push(this.searchHdfs(filename, list));
        }

        await Promise.all(actions);

        return slice;
    }
}
