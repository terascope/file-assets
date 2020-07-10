import {
    RouteSenderAPI,
    DataEntity,
    AnyObject,
    Logger,
    TSError,
    pMap
} from '@terascope/job-components';
import ChunkedSender from '../__lib/chunked-file-sender';
import { FileSenderType } from '../__lib/interfaces';

export default class S3Sender extends ChunkedSender implements RouteSenderAPI {
    logger: Logger;
    concurrency: number;
    client: AnyObject;

    constructor(client: AnyObject, config: AnyObject, logger: Logger) {
        super(FileSenderType.file, config as any);
        this.logger = logger;
        const { concurrency } = config;
        this.concurrency = concurrency;
        this.client = client;
    }

    private async sendToS3(filename: string, list: DataEntity[]): Promise<any> {
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

    async send(records: DataEntity[]):Promise<void> {
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
            ([fileName, list]) => this.sendToS3(fileName, list),
            { concurrency }
        );
    }

    async verify(_route?: string): Promise<void> {
        const { path } = this.config;
        const { bucket } = parsePath(path);
        const query = { Bucket: bucket };

        try {
            await this.client.headBucket_Async(query);
        } catch (_err) {
            try {
                await this.client.createBucket_Async(query);
            } catch (err) {
                throw new TSError(err, { reason: `Could not setup bucket ${path}}` });
            }
        }
    }
}
