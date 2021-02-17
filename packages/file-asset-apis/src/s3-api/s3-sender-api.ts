import {
    RouteSenderAPI,
    DataEntity,
    AnyObject,
    Logger,
    TSError,
    pMap
} from '@terascope/job-components';
import { parsePath, ChunkedFileSender } from '../lib';
import { FileSenderType, S3PutConfig, ChunkedSenderConfig } from '../interfaces';

export class S3Sender extends ChunkedFileSender implements RouteSenderAPI {
    logger: Logger;
    concurrency: number;
    client: AnyObject;

    constructor(client: AnyObject, config: ChunkedSenderConfig, logger: Logger) {
        super(FileSenderType.s3, config as any);
        this.logger = logger;
        const { concurrency } = config;
        this.concurrency = concurrency;
        this.client = client;
    }

    private async sendToS3(
        file: string, list: (DataEntity | Record<string, unknown>)[]
    ): Promise<any> {
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

    /**
     * Write data to file
     *
     * @example
     * s3Sender.send([{ some: 'data' }]) => Promise<void>
     * s3Sender.send([DataEntity.make({ some: 'data' })]) => Promise<void>
    */
    async send(records: (DataEntity | Record<string, unknown>)[]):Promise<void> {
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
            ([fileName, list]) => this.sendToS3(fileName, list),
            { concurrency }
        );
    }

    /**
     * Used to verify that the bucket exists, will throw if it does not exist
     */
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
    /**
     * This is currently a noop, may change in future and work to ensure dynamic bucket exists
     */
    async verify(_route: string): Promise<void> {}
}
