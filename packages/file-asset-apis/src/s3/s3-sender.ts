import { Logger, TSError, RouteSenderAPI } from '@terascope/utils';
import type { S3Client } from './client-helpers/index.js';
import {
    parsePath, ChunkedFileSender, SendBatchConfig
} from '../base/index.js';
import { FileSenderType, ChunkedFileSenderConfig } from '../interfaces.js';
import {
    createS3Bucket,
    headS3Bucket,
    putS3Object,
} from './s3-helpers.js';
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

                // speeds up by a couple seconds but not as much as hoped
                // the index is zero based but the part numbers start at 1
                // so we need to increment by 1
                uploader.enqueuePartSync(Body, chunk.index + 1);
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
}

// DIDNT WORK
// pMapIterable - think has to run at chunkGenerator but might throw off the index i think
// async function* source() {
//     yield 1;
//     yield 2;
//     yield 3;
// }

// const target = pMapIterable(source(), async (n) => {
//     console.log(`Running with ${n}...`);
//     await pDelay(1000);
//     console.log(`Finished running with ${n}`);
// }, {
//     concurrency: 1,
//     backpressure: 2
// });

// for await (const _ of target) {}

// WORKED BUT DECIDED NO
// maybe faster - but seems like collecting a few chunks in memory could be bad
// async sendToDestination3({ filename, chunkGenerator }: SendBatchConfig): Promise<void> {
//     const objPath = parsePath(filename);
//     const Key = await this.createFileDestinationName(objPath.prefix);
//     const Bucket = objPath.bucket;

//     let isFirstSlice = true;
//     // docs say Body can be a string, but types are complaining
//     let Body: any;
//     let uploader: MultiPartUploader | undefined;

//     let items = [];
//     let pending = 0;

//     try {
//         for await (const chunk of chunkGenerator) {
//             Body = chunk.data;

//             // first slice decides if it is multipart or not
//             if (isFirstSlice) {
//                 isFirstSlice = false;

//                 if (chunk.has_more) {
//                     uploader = new MultiPartUploader(
//                         this.client, Bucket, Key, this.logger
//                     );
//                     await uploader.start();
//                 } else {
//                 // make regular query
//                     if (!Body) return;

//                     await putS3Object(this.client, {
//                         Bucket,
//                         Key,
//                         Body
//                     });
//                     return;
//                 }
//             }
//             if (!uploader) throw new Error('Expected uploader to exist');

//             items.push([Body, chunk.index + 1]);
//             if (items.length > 5) {
//                 pending++;
//                 Promise.all(
//                     items.map(([body, part]) => uploader!.enqueuePartAsync(body, part))
//                 ).then(() => {
//                     pending--;
//                 });
//                 items = [];
//             }
//         }

//         await pWhile(async () => pending === 0);

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
