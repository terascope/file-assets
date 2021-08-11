import type S3 from 'aws-sdk/clients/s3';
import {
    Logger, pDelay, pWhile, sortBy, toHumanTime
} from '@terascope/utils';
import {
    createS3MultipartUpload,
    uploadS3ObjectPart,
    finalizeS3Multipart
} from './s3-helpers';

/**
 * This is a multi-part uploader that will handle
 * uploading the parts in the background
*/
export class MultiPartUploader {
    /**
     * This will be set once start is called
    */
    private uploadId: string|undefined;

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

    private finishing = false;

    constructor(
        readonly client: S3,
        readonly bucket: string,
        readonly key: string,
        readonly logger: Logger
    ) {}

    /**
     * Start the multi-part upload
    */
    async start(): Promise<void> {
        this.uploadId = await createS3MultipartUpload(
            this.client, this.bucket, this.key
        );
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

        this._uploadPart(body, partNumber)
            .catch((err) => {
                this.partUploadErrors.set(String(err), err);
            }).finally(() => {
                this.pendingParts--;
            });
    }

    /**
     * Make the s3 part upload request
    */
    private async _uploadPart(body: Buffer, partNumber: number): Promise<void> {
        if (!this.uploadId) {
            throw Error('Expected MultiPartUploader->start to have been called');
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
        if (!this.uploadId) {
            throw Error('Expected MultiPartUploader->start to have been called');
        }

        this.finishing = true;
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
            UploadId: this.uploadId
        });
        this.logger.debug(`finalizeS3Multipart(${this.bucket}, ${this.key}, ${this.uploadId}) took ${toHumanTime(Date.now() - start)}`);
    }

    /**
     * Used before finalizing all the multiple part upload
     * to ensure all of the part requests are done and to
     * throw an error if needed.
     *
     * Ideally this should wait for any pending requests
     * to finish, even if there is already an error, this
     * is to avoid leaving dangling requests
    */
    private async _waitForParts(): Promise<void> {
        if (this.pendingParts > 0) {
            await pWhile(async () => {
                await pDelay(100);
                return this.pendingParts === 0;
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
