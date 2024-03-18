import {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand, DeleteObjectCommand, DeleteObjectsCommand,
    GetObjectCommand, HeadBucketCommand,
    ListBucketsCommand, ListObjectsV2Command, ObjectIdentifier,
    PutObjectCommand, PutObjectTaggingCommand,
    CreateMultipartUploadCommand, UploadPartCommand,
    CompleteMultipartUploadCommand, AbortMultipartUploadCommand,
} from '@aws-sdk/client-s3';

import { TSError, pDelay, AnyObject } from '@terascope/utils';
import { S3ClientParams, S3ClientResponse, S3RetryRequest } from './client-types';

export async function s3RequestWithRetry(
    retryArgs: S3RetryRequest.GetObjectWithRetry,
    attempts?: number
): Promise<S3ClientResponse.GetObjectCommandOutput>
export async function s3RequestWithRetry(
    retryArgs: S3RetryRequest.PutObjectWithRetry,
    attempts?: number
): Promise<S3ClientResponse.PutObjectCommandOutput>
export async function s3RequestWithRetry(
    retryArgs: S3RetryRequest.DeleteObjectWithRetry,
    attempts?: number
): Promise<S3ClientResponse.DeleteObjectOutput>
export async function s3RequestWithRetry(
    retryArgs: S3RetryRequest.ListObjectsWithRetry,
    attempts?: number
): Promise<S3ClientResponse.ListObjectsV2Output>
export async function s3RequestWithRetry(
    retryArgs: S3RetryRequest.RetryArgs,
    attempts = 1
): Promise<S3RetryRequest.S3RetryResponse> {
    const {
        client,
        func,
        params
    } = retryArgs;

    try {
        const results = await func(client, params);

        return results;
    } catch (e: unknown) {
        let retry = false;
        // check if it's an aws issue
        if ((e as AnyObject).$metadata?.httpStatusCode === 503
            || (e as AnyObject).$metadata?.httpStatusCode === 500
            // check if it's a server error
            || (e as Error).message.includes('ENOTFOUND')
            || (e as Error).message.includes('EAI_AGAIN')) {
            retry = true;
        }

        if (retry && attempts < 4) {
            await pDelay(250 * attempts);
            return s3RequestWithRetry(retryArgs, attempts + 1);
        }

        throw new TSError(e);
    }
}

export async function getS3Object(
    client: S3Client,
    params: S3ClientParams.GetObjectRequest
): Promise<S3ClientResponse.GetObjectCommandOutput> {
    const command = new GetObjectCommand(params);
    return client.send(command);
}

export async function listS3Objects(
    client: S3Client,
    params: S3ClientParams.ListObjectsV2Request,
): Promise<S3ClientResponse.ListObjectsV2Output> {
    const command = new ListObjectsV2Command(params);
    return client.send(command);
}

export async function putS3Object(
    client: S3Client,
    params: S3ClientParams.PutObjectRequest
): Promise<S3ClientResponse.PutObjectCommandOutput> {
    const command = new PutObjectCommand(params);
    return client.send(command);
}

export async function tagS3Object(
    client: S3Client,
    params: S3ClientParams.PutObjectTaggingRequest
): Promise<S3ClientResponse.PutObjectTaggingOutput> {
    const command = new PutObjectTaggingCommand(params);
    return client.send(command);
}

export async function deleteS3Object(
    client: S3Client,
    params: S3ClientParams.DeleteObjectRequest
): Promise<S3ClientResponse.DeleteObjectOutput> {
    const command = new DeleteObjectCommand(params);
    return client.send(command);
}

/** Deletes up to 10000 or MaxKeys, if you want to delete more use {@link deleteAllS3Objects} */
export async function deleteS3Objects(
    client: S3Client,
    params: S3ClientParams.DeleteObjectsRequest,
): Promise<S3ClientResponse.DeleteObjectsOutput> {
    const command = new DeleteObjectsCommand(params);
    return client.send(command);
}

/** Lists objects and continues deleting until empty */
export async function deleteAllS3Objects(
    client: S3Client,
    params: S3ClientParams.ListObjectsV2Request
): Promise<void> {
    const list = await listS3Objects(client, params);
    if (!list.Contents?.length) return;

    const objects: ObjectIdentifier[] = [];
    list.Contents?.forEach((obj) => {
        if (!obj.Key) return;
        objects.push({ Key: obj.Key });
    });

    await deleteS3Objects(client, {
        Bucket: params.Bucket,
        Delete: { Objects: objects }
    });

    if (list.NextContinuationToken) {
        return deleteAllS3Objects(
            client, { ...params, ContinuationToken: list.NextContinuationToken }
        );
    }
}

export async function deleteS3Bucket(
    client: S3Client,
    params: S3ClientParams.DeleteBucketRequest
): Promise<void> {
    const command = new DeleteBucketCommand(params);
    await client.send(command);
}

export async function headS3Bucket(
    client: S3Client,
    params: S3ClientParams.HeadBucketRequest
): Promise<void> {
    const command = new HeadBucketCommand(params);
    await client.send(command);
}

export async function doesBucketExist(
    client: S3Client,
    params: S3ClientParams.HeadBucketRequest
): Promise<boolean> {
    try {
        await headS3Bucket(client, params);
    } catch (err) {
        const { httpStatusCode } = (err as S3ClientResponse.S3ErrorExceptions).$metadata ?? {};

        if (httpStatusCode === 404) {
            return false;
        }
        if (httpStatusCode === 403) {
            throw new TSError(`User does not have access to bucket "${params.Bucket}"`, { statusCode: 403 });
        }
        throw err;
    }
    return true;
}

export async function listS3Buckets(
    client: S3Client,
): Promise<S3ClientResponse.ListBucketsOutput> {
    const command = new ListBucketsCommand({});
    return client.send(command);
}

export async function createS3Bucket(
    client: S3Client,
    params: S3ClientParams.CreateBucketRequest
): Promise<S3ClientResponse.CreateBucketOutput> {
    const command = new CreateBucketCommand(params);
    return client.send(command);
}

export async function createS3MultipartUpload(
    client: S3Client,
    Bucket: string,
    Key: string
): Promise<string> {
    const multiPartPayload = {
        Bucket,
        Key
    };
    const command = new CreateMultipartUploadCommand(multiPartPayload);

    const resp = await client.send(command);

    return resp.UploadId as string;
}

export async function uploadS3ObjectPart(
    client: S3Client,
    params: S3ClientParams.UploadPartRequest
): Promise<S3ClientResponse.CompletedPart> {
    const command = new UploadPartCommand(params);
    return client.send(command);
}

export async function finalizeS3Multipart(
    client: S3Client,
    params: S3ClientParams.CompleteMultipartUploadRequest
): Promise<void> {
    const command = new CompleteMultipartUploadCommand(params);
    await client.send(command);
}

export async function abortS3Multipart(
    client: S3Client,
    params: S3ClientParams.AbortMultipartUploadRequest
): Promise<void> {
    const command = new AbortMultipartUploadCommand(params);
    await client.send(command);
}
