import { Logger, TSError, RouteSenderAPI, pWhile } from '@terascope/utils';
import type { S3Client } from './client-helpers/index.js';
import {
    parsePath, ChunkedFileSender, SendBatchConfig, MiB
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
     *
     */
    protected async sendToDestination(
        { filename, chunkGenerator }: SendBatchConfig
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

                if (!uploader) throw new Error('Expected uploader to exist');

                // don't allow too many concurrent uploads to prevent holding a lot in memory
                const queueSize = chunkGenerator.chunkSize > (100 * MiB) ? 1 : 5;
                if (pending > queueSize) {
                    await pWhile(async () => pending <= queueSize);
                }

                pending++;

                // the index is zero based but the part numbers start at 1
                // so we need to increment by 1
                uploader
                    .enqueuePart(
                        Body, chunk.index + 1
                    )
                    .finally(() => {
                        pending--;
                    });
            }

            // we are done, finalize the upload
            // do this outside of the for loop
            // to free up the chunkGenerator
            if (uploader) {
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

    /**
     * similar to simpleSend but but more of a factory for the
     * user to decide how to provide the records instead of
     * chunking
    */
    async sendNonChunked({
        recordsPerUpload, chunks, size, getRecords, doesHaveMore
    }: {
        recordsPerUpload: number;
        chunks: number;
        size: number;
        getRecords: (start?: number, end?: number) => any[];
        doesHaveMore: (start?: number) => boolean;
    }) {
        const objPath = parsePath(this.path);
        const Key = await this.createFileDestinationName(objPath.prefix);
        const Bucket = objPath.bucket;

        let uploader: MultiPartUploader | undefined;
        let pending = 0;

        let isFirstSlice = true;
        let start = 0;
        let index = 0;

        const twoHundredMiB = MiB * 200;
        const oneHundredMiB = MiB * 100;

        let hasMore = true;

        // don't allow too many concurrent uploads to prevent holding a lot in memory
        const queueSize = 5;

        const send = async (idx: number, isMore: boolean, body: any) => {
            // first slice decides if it is multipart or not
            if (isFirstSlice) {
                isFirstSlice = false;

                if (isMore) {
                    uploader = new MultiPartUploader(
                        this.client, Bucket, Key, this.logger
                    );
                    await uploader.start();
                } else {
                    if (!body) return;

                    await putS3Object(this.client, {
                        Bucket,
                        Key,
                        Body: body
                    });
                    return;
                }
            }

            if (!uploader) throw new Error('Expected uploader to exist');

            if (pending > queueSize) {
                await pWhile(async () => pending <= queueSize);
            }

            pending++;

            uploader
                .enqueuePart(
                    body, idx
                )
                .finally(() => {
                    pending--;
                });
        };

        while (hasMore) {
            let end = recordsPerUpload + start;
            if (end > size) end = size;

            const ary = getRecords(start, end);

            const body = this.formatter.format(ary);
            // const body = ary.map((el) => JSON.stringify(el)).join('\n');

            // the index is zero based but the part numbers start at 1
            // so we need to increment by 1
            index = index + 1;

            // should be under 1MiB hopefully but allow up to 2MiB
            if (body.length < twoHundredMiB) {
                start = start + recordsPerUpload;

                if (chunks === 1 || start > size) {
                    hasMore = false;
                } else {
                    hasMore = doesHaveMore(start);
                }

                await send(index, hasMore, body);
            } else {
                // in case estimates went off limit,
                // TODO maybe split ary in half, pop off a few at a time,
                // or estimate overflow, instead of looping each record

                let str = '';
                let recordsProcessed = 0;

                for (const record of ary) {
                    recordsProcessed = recordsProcessed + 1;
                    str = str + `${JSON.stringify(record)}\n`;

                    if (str.length >= oneHundredMiB) {
                        start = start + recordsProcessed;

                        if (start > size) {
                            hasMore = false;
                        } else {
                            hasMore = doesHaveMore(start);
                        }

                        await send(index, hasMore, str);

                        break;
                    }
                }
                start = start + recordsProcessed;

                if (start > size) {
                    hasMore = false;
                } else {
                    hasMore = doesHaveMore(start);
                }

                await send(index, hasMore, str);
            }
        }

        if (uploader) {
            return await uploader.finish();
        }
    }
}
