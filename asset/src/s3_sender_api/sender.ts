import {
    RouteSenderAPI,
    DataEntity,
    AnyObject,
    Logger,
    TSError,
    pMap
} from '@terascope/job-components';
import { parsePath } from '../s3_reader/helpers';
import ChunkedSender from '../__lib/chunked-file-sender';
import { FileSenderType } from '../__lib/interfaces';
import { S3PutConfig, S3ExporterAPIConfig } from './interfaces';

export default class S3Sender extends ChunkedSender implements RouteSenderAPI {
    logger: Logger;
    concurrency: number;
    client: AnyObject;

    constructor(client: AnyObject, config: S3ExporterAPIConfig, logger: Logger) {
        super(FileSenderType.s3, config as any);
        this.logger = logger;
        const { concurrency } = config;
        this.concurrency = concurrency;
        this.client = client;
    }

    private async sendToS3(file: string, list: DataEntity[]): Promise<any> {
        const objPath = parsePath(file);

        const { fileName, output } = await this.prepareSegment(objPath.prefix, list);

        // This will prevent empty objects from being added to the S3 store, which can cause
        // problems with the S3 reader
        if (!output || output.length === 0) {
            return [];
        }
        // TODO: may need to verify bucket first
        const params: S3PutConfig = {
            Bucket: objPath.bucket,
            Key: fileName,
            Body: output as string
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

    async ensureBucket(route: string): Promise<void> {
        const { bucket } = parsePath(route);
        const query = { Bucket: bucket };

        try {
            await this.client.headBucket_Async(query);
        } catch (_err) {
            try {
                await this.client.createBucket_Async(query);
            } catch (err) {
                throw new TSError(err, { reason: `Could not setup bucket ${route}}` });
            }
        }
    }

    // TODO: for now this will not be used as we are still unclear how
    // routing to multiple buckets will work
    async verify(_route: string): Promise<void> {}
}
