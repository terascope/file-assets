import type S3 from 'aws-sdk/clients/s3';
import { Logger, toHumanTime } from '@terascope/utils';
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

    constructor(
        readonly client: S3,
        readonly bucket: string,
        readonly key: string,
        readonly logger: Logger
    ) {

    }

    /**
     * Start the multi-part upload
    */
    async start(): Promise<void> {
        this.uploadId = await createS3MultipartUpload(
            this.client, this.bucket, this.key
        );
    }

    /**
     * Make the s3 part upload request
    */
    async uploadPart(body: Buffer, partNumber: number): Promise<void> {
        if (!this.uploadId) {
            throw Error('Expected MultiPartUploader->start to have been called');
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

        const start = Date.now();
        // Finalize multipart upload
        await finalizeS3Multipart(this.client, {
            Bucket: this.bucket,
            Key: this.key,
            MultipartUpload: {
                Parts: this.parts
            },
            UploadId: this.uploadId
        });
        this.logger.debug(`finalizeS3Multipart(${this.bucket}, ${this.key}, ${this.uploadId}) took ${toHumanTime(Date.now() - start)}`);
    }
}
