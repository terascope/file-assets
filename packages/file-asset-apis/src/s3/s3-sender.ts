import {
    Logger, TSError, RouteSenderAPI, pWhile, pDelay,
} from '@terascope/utils';
import type { S3Client } from './client-helpers/index.js';
import {
    parsePath, ChunkedFileSender, SendBatchConfig // Chunk,
} from '../base/index.js';
import { FileSenderType, ChunkedFileSenderConfig } from '../interfaces.js';
import { createS3Bucket, headS3Bucket, putS3Object } from './s3-helpers.js';
import { isObject } from '../helpers.js';
import { MultiPartUploader } from './MultiPartUploader.js';
// import { pMapIterable } from 'p-map';

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

    protected async sendToDestination(
        { filename, chunkGenerator, concurrency = 1 }: SendBatchConfig
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
                if (pending > concurrency) {
                    await pWhile(async () => {
                        // TODO change pWhile to not force timeout w/jitter OR add a delay option
                        await pDelay(100);
                        return pending <= concurrency;
                    });
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
                if (pending > 0) {
                    await pWhile(async () => {
                        // TODO change pWhile to not force timeout w/jitter OR add a delay option
                        await pDelay(100);
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

    // alt option for handling concurrency - close but not quite right
    // protected async sendToDestinationPMapIter(
    //     { filename, chunkGenerator }: SendBatchConfig
    // ): Promise<void> {
    //     const objPath = parsePath(filename);
    //     const Key = await this.createFileDestinationName(objPath.prefix);
    //     const Bucket = objPath.bucket;

    //     let isFirstSlice = true;
    //     // docs say Body can be a string, but types are complaining
    //     let Body: any;
    //     let uploader: MultiPartUploader | undefined;

    //     const mapper = async (chunk: Chunk) => {
    //         Body = chunk.data;

    //         // first slice decides if it is multipart or not
    //         if (isFirstSlice) {
    //             isFirstSlice = false;

    //             if (chunk.has_more) {
    //                 uploader = new MultiPartUploader(
    //                     this.client, Bucket, Key, this.logger
    //                 );
    //                 console.error('===uploaderFirst');
    //                 await uploader.start();
    //             } else {
    //                 // make regular query
    //                 if (!Body) return;

    //                 console.error('===put1');
    //                 await putS3Object(this.client, {
    //                     Bucket,
    //                     Key,
    //                     Body
    //                 });
    //                 return;
    //             }
    //         } else {
    //             console.error('===uploaderFirstNO');
    //         }

    //         if (!uploader) throw new Error('Expected uploader to exist');

    //         if (!chunk.has_more) {
    //             console.error('===idx no more', chunk.index, uploader.pendingParts);
    //             await pWhile(async () => {
    //                 await pDelay(100);
    //                 if (uploader!.pendingParts < 1) return true;
    //             });
    //         } else {
    //             console.error('===idx', chunk.index, uploader.pendingParts);
    //         }
    //         // the index is zero based but the part numbers start at 1
    //         // so we need to increment by 1
    //         await uploader.enqueuePart(
    //             Body, chunk.index + 1
    //         );
    //     };

    //     try {
    //         const start = Date.now();
    //         // Multiple posts are fetched concurrently, with limited concurrency and backpressure
    //         for await (const _chunk of pMapIterable(
    //             chunkGenerator,
    //             mapper,
    //             { concurrency: 50 })
    //         ) {
    //             console.log('post,_chunk', _chunk);
    //         }
    //         console.error('===2', Date.now() - start);

    //         if (uploader) {
    //             return await uploader.finish();
    //         }
    //     } catch (err) {
    //         if (uploader) {
    //             await uploader.abort();
    //         }
    //         throw err;
    //     }
    // }

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
