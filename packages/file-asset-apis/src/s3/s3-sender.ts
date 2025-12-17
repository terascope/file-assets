import {
    Logger, TSError,
    pWhile, pDelay,
} from '@terascope/core-utils';
import { RouteSenderAPI } from '@terascope/job-components';
import type { S3Client } from './client-helpers/index.js';
import {
    parsePath, ChunkedFileSender, SendBatchConfig
} from '../base/index.js';
import { FileSenderType, ChunkedFileSenderConfig } from '../interfaces.js';
import { createS3Bucket, headS3Bucket, putS3Object } from './s3-helpers.js';
import { isObject } from '../helpers.js';
import { MultiPartUploader } from './MultiPartUploader.js';

function validateConfig(input: unknown) {
    if (!isObject(input)) {
        throw new Error('Invalid config parameter, ut must be an object');
    }

    if (input.file_per_slice == null || input.file_per_slice === false) {
        throw new Error('Invalid parameter "file_per_slice", it must be set to true, cannot be append data to S3 objects');
    }
}

export class S3Sender extends ChunkedFileSender implements RouteSenderAPI {
    client: S3Client;

    constructor(client: S3Client, config: ChunkedFileSenderConfig, logger: Logger) {
        validateConfig(config);
        super(FileSenderType.s3, config, logger);
        this.client = client;
    }

    /**
     * This is a low level API, it is not meant to be used externally,
     * please use the "send" method instead
     */
    protected async sendToDestination(
        { filename, chunkGenerator, concurrency = 1, jitter = 10 }: SendBatchConfig
    ): Promise<void> {
        const objPath = parsePath(filename);
        const Key = await this.createFileDestinationName(objPath.prefix);
        const Bucket = objPath.bucket;

        let isFirstSlice = true;
        // docs say Body can be a string, but types are complaining
        let Body: any;
        let uploader: MultiPartUploader | undefined;
        let pending = 0;

        try {
            for await (const chunk of chunkGenerator) {
                Body = chunk.data;

                // first slice decides if it is multipart or not
                if (isFirstSlice) {
                    isFirstSlice = false;

                    if (chunk.has_more) {
                        uploader = new MultiPartUploader(
                            this.client, Bucket, Key, this.logger
                        );
                        await uploader.start();
                    } else {
                        if (!Body) {
                            this.logger.error(`Nothing to send to ${filename}'`);
                            return;
                        }

                        await putS3Object(this.client, {
                            Bucket,
                            Key,
                            Body
                        });
                        return;
                    }
                }

                if (!uploader) throw new Error('Expected uploader to exist');

                pending++;
                uploader
                    .enqueuePart(
                        Body,
                        // index is zero based but part numbers start at 1 so increment by 1
                        chunk.index + 1
                    )
                    .finally(() => {
                        pending--;
                    });

                if (pending >= concurrency) {
                    await pWhile(async () => {
                        await pDelay(jitter);
                        return pending < concurrency;
                    });
                }
            }

            // we are done, finalize the upload
            // do this outside of the for loop
            // to free up the chunkGenerator
            if (uploader) {
                if (pending > 0) {
                    await pWhile(async () => {
                        await pDelay(jitter);
                        return pending === 0;
                    });
                }
                return await uploader.finish();
            }
        } catch (err) {
            if (uploader) {
                await uploader.abort();
            }
            throw err;
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
                // Unclear TypeError when trying to connect to a secure S3 bucket over http
                if (err instanceof TypeError && err.message.includes('Cannot read properties of undefined (reading \'#text\')')) {
                    throw new TSError(err, {
                        reason: `Failure connecting to secure bucket ${this.path} from an http endpoint`
                    });
                }
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
