import { EventEmitter, once } from 'events';
import type S3 from 'aws-sdk/clients/s3';
import { E_CANCELED, Semaphore } from 'async-mutex';
import {
    Logger, sortBy, toHumanTime
} from '@terascope/utils';
import {
    createS3MultipartUpload,
    uploadS3ObjectPart,
    finalizeS3Multipart
} from './s3-helpers';

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
    private uploadId: string|undefined;

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
    private parts: S3.CompletedPart[] = [];

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
     * this is used to control the concurrency of the
     * part upload requests
    */
    private readonly readSemaphore: Semaphore;
    private readonly events: EventEmitter;

    constructor(
        readonly client: S3,
        readonly bucket: string,
        readonly key: string,
        readonly concurrency: number,
        readonly logger: Logger
    ) {
        this.readSemaphore = new Semaphore(this.concurrency);
        this.events = new EventEmitter();
    }

    /**
     * Start the multi-part upload
    */
    start(): void {
        if (this.started) throw new Error('Upload already started');

        const start = Date.now();
        this.started = true;
        createS3MultipartUpload(
            this.client, this.bucket, this.key
        ).then((uploadId) => {
            this.uploadId = uploadId;
            this.logger.debug(`s3 multipart upload ${uploadId} started, took ${toHumanTime(Date.now() - start)}`);
        }).catch((err) => {
            this.startError = err;
        }).finally(() => {
            this.events.emit(Events.StartDone);
        });
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
    enqueuePart(body: Buffer, partNumber: number): void {
        if (this.finishing) {
            throw new Error(`MultiPartUploader already finishing, cannot upload part #${partNumber}`);
        }

        if (this.partUploadErrors.size) {
            this._throwPartUploadError();
        }

        this.pendingParts++;
        this.readSemaphore.runExclusive(async () => {
            await this._waitForStart(`part #${partNumber}`);
            await this._uploadPart(body, partNumber);
        }).catch((err) => {
            if (err === E_CANCELED) {
                this.logger.debug(`upload part #${partNumber} canceled`);
                return;
            }

            this.partUploadErrors.set(String(err), err);
            this.readSemaphore.cancel();
        }).finally(() => {
            this.pendingParts--;
            this.events.emit(Events.PartDone);
        });
    }

    /**
     * Make the s3 part upload request
    */
    private async _uploadPart(body: Buffer, partNumber: number): Promise<void> {
        if (!this.uploadId) {
            throw Error('Expected MultiPartUploader->start to have been finished');
        }

        if (this.partUploadErrors.size) {
            this._throwPartUploadError();
        }

        this.parts.push(await uploadS3ObjectPart(this.client, {
            Bucket: this.bucket,
            Key: this.key,
            Body: body,
            UploadId: this.uploadId,
            PartNumber: partNumber
        }));
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
            this._throwPartUploadError();
        }
    }

    private _throwPartUploadError(): never {
        if (this.partUploadErrors.size === 0) {
            throw new Error('Expected a part upload error');
        }
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
}
