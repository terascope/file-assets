import type { RouteSenderAPI } from '@terascope/job-components';
import type S3 from 'aws-sdk/clients/s3';
import {
    DataEntity,
    Logger,
    TSError
} from '@terascope/utils';
import { parsePath, ChunkedFileSender } from '../base';
import { FileSenderType, S3PutConfig, ChunkedSenderConfig } from '../interfaces';
import { createS3Bucket, headS3Bucket, putS3Object } from './s3-helpers';
import { isObject } from '../helpers';

function validateConfig(input: unknown) {
    if (!isObject(input)) throw new Error('Invalid config parameter, ut must be an object');
    (input as Record<string, unknown>);
    if (input.file_per_slice == null || input.file_per_slice === false) {
        throw new Error('Invalid parameter "file_per_slice", it must be set to true, cannot be append data to S3 objects');
    }
}

export class S3Sender extends ChunkedFileSender implements RouteSenderAPI {
    logger: Logger;
    client: S3;

    constructor(client: S3, config: ChunkedSenderConfig, logger: Logger) {
        validateConfig(config);
        super(FileSenderType.s3, config);
        this.logger = logger;
        this.client = client;
    }

    /**
     * This is a low level API, it is not meant to be used externally,
     * please use the "send" method instead
     *
     */
    protected async sendToDestination(
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
            Body: output
        };

        return putS3Object(this.client, params);
    }

    /**
     * Used to verify that the bucket exists, will throw if it does not exist
     */
    async ensureBucket(route: string): Promise<void> {
        const { bucket } = parsePath(route);
        const params = { Bucket: bucket };

        try {
            await headS3Bucket(this.client, params);
        } catch (_err) {
            try {
                await createS3Bucket(this.client, params);
            } catch (err) {
                throw new TSError(err, {
                    reason: `Could not setup bucket ${route}`
                });
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
