import { EventEmitter, once } from 'node:events';
import {
    Logger, pDelay, pWhile, sortBy, toHumanTime
} from '@terascope/utils';
import type { S3Client, S3ClientResponse } from './client-helpers/index.js';
import {
    createS3MultipartUpload,
    uploadS3ObjectPart,
    finalizeS3Multipart,
    abortS3Multipart
} from './s3-helpers.js';

enum Events {
    StartDone = 'start:done',
    PartDone = 'part:done',
}

/**
 * This is a multi-part uploader that will handle
 * uploading the parts in the background.
*/
export class MultiPartUploader {
    /**
     * This will be set once start is called
    */
    private uploadId: string | undefined;

    /**
     * This will be used to throw an error if any new
     * parts are added or finish is called before start is called
    */
    private started = false;

    /**
     * If there is an error starting this should happen here
    */
    private startError: unknown;

    /**
     * These are the completed responses from the upload
     * part requests
    */
    private parts: S3ClientResponse.CompletedPart[] = [];

    /**
     * This is a way of tracking the number of pending
     * part requests, this should be decremented for either
     * a failed or successful request
    */
    private pendingParts = 0;

    private partUploadErrors = new Map<string, unknown>();

    /**
     * This will be used to throw an error if any new
     * parts are added after finishing is called
    */
    private finishing = false;

    /**
     * This will be used to check to see if we should
     * abort the request
    */
    private finished = false;

    private readonly events: EventEmitter;
    private readonly client: S3Client;

    constructor(
        client: S3Client,
        readonly bucket: string,
        readonly key: string,
        readonly logger: Logger
    ) {
        this.client = client;
        this.events = new EventEmitter();
        // just so we don't get warnings set this to a higher number
        this.events.setMaxListeners(1000);
    }

    /**
     * Start the multi-part upload
    */
    async start(): Promise<void> {
        if (this.started) throw new Error('Upload already started');

        const start = Date.now();

        this.started = true;
        try {
            const uploadId = await createS3MultipartUpload(
                this.client, this.bucket, this.key
            );

            this.uploadId = uploadId;
            this.logger.debug(`s3 multipart upload ${uploadId} started, took ${toHumanTime(Date.now() - start)}`);
        } catch (err) {
            this.startError = err;
        } finally {
            this.events.emit(Events.StartDone);
        }

        // adding this here will ensure that
        // we give the event loop some time to
        // to start the upload
        await pDelay(0);
    }

    /**
     * Used wait until the background request for start is finished.
     * If that failed, this should throw
    */
    private async _waitForStart(ctx: string): Promise<void> {
        if (!this.started) {
            throw Error('Expected MultiPartUploader->start to have been finished');
        }

        if (this.uploadId == null) {
            this.logger.debug(`${ctx} waiting for upload to start`);
            await once(this.events, Events.StartDone);
        }

        if (this.startError != null) {
            throw this.startError;
        }
    }

    /**
     * Enqueue a part upload request
    */
    async enqueuePart(
        body: Buffer | string, partNumber: number
    ): Promise<void> {
        if (this.finishing) {
            throw new Error(`MultiPartUploader already finishing, cannot upload part #${partNumber}`);
        }

        if (this.partUploadErrors.size) {
            await this._throwPartUploadError();
        }
        this.pendingParts++;

        // run in background so more than 1 part can upload at a time
        Promise.resolve()
            .then(() => this._waitForQueue())
            .then(() => this._waitForStart(`part #${partNumber}`))
            .then(() => this._uploadPart(body, partNumber))
            .catch((err) => {
                this.partUploadErrors.set(String(err), err);
            })
            .finally(async () => {
                this.pendingParts--;
                this.events.emit(Events.PartDone);

                if (this.pendingParts > 0 || !this.uploadId) {
                    // adding this here will ensure that
                    // we give the event loop some time to
                    // to start the upload
                    await pDelay(this.pendingParts);
                }
            });
    }

    /**
     * no hard limit on concurrent uploads to S3, read good to keep below 1,000
     * but decided to limit further to 100 to be safe
     */
    private async _waitForQueue() {
        const concurrency = 100;
        if (this.pendingParts <= concurrency) return;
        return pWhile(async () => this.pendingParts <= concurrency);
    }

    /**
     * Make the s3 part upload request
    */
    private async _uploadPart(body: Buffer | string, partNumber: number): Promise<void> {
        if (!this.uploadId) {
            throw Error('Expected MultiPartUploader->start to have been finished');
        }

        if (this.partUploadErrors.size) {
            await this._throwPartUploadError();
        }
        const { ETag } = await uploadS3ObjectPart(this.client, {
            Bucket: this.bucket,
            Key: this.key,
            Body: body as any,
            UploadId: this.uploadId,
            PartNumber: partNumber
        });

        this.parts.push({ PartNumber: partNumber, ETag });
    }

    /**
     * Finish the multi-part upload, no parts should be uploaded
     * after this called
    */
    async finish(): Promise<void> {
        this.finishing = true;
        await this._waitForStart('finish');
        await this._waitForParts();

        const start = Date.now();
        await finalizeS3Multipart(this.client, {
            Bucket: this.bucket,
            Key: this.key,
            MultipartUpload: {
                // if we don't sort the parts, the upload
                // request will fail
                Parts: sortBy(this.parts, 'PartNumber')
            },
            UploadId: this.uploadId!
        });
        this.finished = true;
        this.logger.debug(`finalizeS3Multipart(${this.bucket}, ${this.key}, ${this.uploadId}) took ${toHumanTime(Date.now() - start)}`);
    }

    /**
     * Used wait until the pending part count is less than or equal to
     * a specific number and to throw an error if needed.
     *
     * Ideally this should wait for any pending requests
     * to finish, even if there is already an error, this
     * is to avoid leaving dangling requests
    */
    private async _waitForParts(minCount = 0): Promise<void> {
        if (this.pendingParts > minCount) {
            this.logger.debug(`Waiting for ~${this.pendingParts} parts to finish uploading...`);
            await new Promise<void>((resolve) => {
                const onPart = () => {
                    if (this.pendingParts <= minCount) {
                        this.events.removeListener(Events.PartDone, onPart);
                        resolve();
                    }
                };
                this.events.on(Events.PartDone, onPart);
            });
        }

        if (this.partUploadErrors.size) {
            await this._throwPartUploadError();
        }
    }

    private async _throwPartUploadError(): Promise<never> {
        if (this.partUploadErrors.size === 0) {
            throw new Error('Expected a part upload error');
        }

        await this.abort();

        if (this.partUploadErrors.size === 1) {
            const [error] = this.partUploadErrors.values();
            throw error;
        }

        const errors = Array.from(this.partUploadErrors.values());
        const errMsg = errors.map((e: any) => e.stack || `${e}`).join(', and');
        const aggError = new Error(`MultiPartUploadErrors: ${errMsg}`);
        // @ts-expect-error
        aggError.errors = errors;
        throw aggError;
    }

    /**
     * Abort the s3 request
    */
    async abort(): Promise<void> {
        if (!this.uploadId || this.finished) return;

        try {
            this.logger.warn('Aborting s3 upload request');
            await abortS3Multipart(this.client, {
                Bucket: this.bucket,
                Key: this.key,
                UploadId: this.uploadId!
            });
        } catch (err) {
            this.logger.warn(err);
        }
    }
}
