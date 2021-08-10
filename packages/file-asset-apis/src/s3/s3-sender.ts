import type { RouteSenderAPI } from '@terascope/job-components';
import type S3 from 'aws-sdk/clients/s3';
import {
    Logger, pMap, toHumanTime, TSError
} from '@terascope/utils';
import {
    parsePath, ChunkedFileSender, SendBatchConfig
} from '../base';
import { FileSenderType, ChunkedFileSenderConfig } from '../interfaces';
import {
    createS3Bucket,
    headS3Bucket,
    putS3Object,
    createS3MultipartUpload,
    uploadS3ObjectPart,
    finalizeS3Multipart
} from './s3-helpers';
import { isObject } from '../helpers';

function validateConfig(input: unknown) {
    if (!isObject(input)) throw new Error('Invalid config parameter, ut must be an object');
    (input as Record<string, unknown>);
    if (input.file_per_slice == null || input.file_per_slice === false) {
        throw new Error('Invalid parameter "file_per_slice", it must be set to true, cannot be append data to S3 objects');
    }
}

export class S3Sender extends ChunkedFileSender implements RouteSenderAPI {
    client: S3;

    constructor(client: S3, config: ChunkedFileSenderConfig, logger: Logger) {
        validateConfig(config);
        super(FileSenderType.s3, config, logger);
        this.client = client;
    }

    /**
     * This is a low level API, it is not meant to be used externally,
     * please use the "send" method instead
     *
     */
    protected async sendToDestination(
        { filename, chunkGenerator } : SendBatchConfig
    ): Promise<void> {
        const objPath = parsePath(filename);
        const Key = await this.createFileDestinationName(objPath.prefix);
        const Bucket = objPath.bucket;

        let isFirstSlice = true;
        let Body: Buffer|undefined;
        let uploadId: string|undefined;
        let partsRequests: S3.UploadPartRequest[] = [];
        const parts: S3.CompletedPart[] = [];

        for await (const chunk of chunkGenerator) {
            Body = chunk.data;
            // first slice decides if it is multipart or not
            if (isFirstSlice) {
                isFirstSlice = false;

                if (chunk.has_more) {
                    // set up multipart
                    uploadId = await createS3MultipartUpload(this.client, Bucket, Key);
                } else {
                    // make regular query
                    if (!Body) return;

                    await putS3Object(this.client, {
                        Bucket,
                        Key,
                        Body
                    });
                    return;
                }
            }

            // since we return if its a regular query, uploadKey will exists
            // if we reach this point
            partsRequests.push({
                Bucket,
                Key,
                Body,
                UploadId: uploadId!,
                PartNumber: chunk.index + 1
            });

            if (!chunk.has_more || partsRequests.length >= this.concurrency) {
                const start = Date.now();

                const requests = partsRequests.slice();
                partsRequests = [];
                parts.push(...(await pMap(
                    requests,
                    (params) => uploadS3ObjectPart(this.client, params),
                    { concurrency: this.concurrency, stopOnError: true }
                )));

                this.logger.debug(`uploadS3ObjectParts(${Bucket}, ${Key}, ${uploadId}), ${requests.length} parts, took ${toHumanTime(Date.now() - start)}`);
            }

            // we are done, finalize the upload
            if (!chunk.has_more) {
                if (partsRequests.length) {
                    throw new Error('Expected partsRequests to be empty');
                }
                const start = Date.now();
                // Finalize multipart upload
                await finalizeS3Multipart(this.client, {
                    Bucket,
                    Key,
                    MultipartUpload: {
                        Parts: parts
                    },
                    UploadId: uploadId!
                });
                this.logger.debug(`finalizeS3Multipart(${Bucket}, ${Key}, ${uploadId}) took ${toHumanTime(Date.now() - start)}`);
            }
        }
    }

    /**
     * Used to verify that the bucket exists, will attempt to create one
     * if it does not exist
     */
    async ensureBucket(): Promise<void> {
        const { bucket } = parsePath(this.path);
        const params = { Bucket: bucket };

        try {
            await headS3Bucket(this.client, params);
        } catch (_err) {
            try {
                await createS3Bucket(this.client, params);
            } catch (err) {
                throw new TSError(err, {
                    reason: `Failure to setup bucket ${this.path}`
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
