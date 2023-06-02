import {
    S3Client, GetObjectCommand, ListObjectsCommand,
    PutObjectCommand, DeleteObjectCommand, DeleteObjectsCommand, DeleteBucketCommand,
    HeadBucketCommand, ListBucketsCommand, CreateBucketCommand,
    CreateMultipartUploadCommand, UploadPartCommand, PutObjectTaggingCommand,
    CompleteMultipartUploadCommand, AbortMultipartUploadCommand, HeadBucketCommandOutput
} from '@aws-sdk/client-s3';

import { S3ClientParams, S3ClientResponse } from './client-types';
import { TSError } from '@terascope/utils';

export async function getS3Object(
    client: S3Client,
    params: S3ClientParams.GetObjectRequest
): Promise<S3ClientResponse.GetObjectOutput> {
    const command = new GetObjectCommand(params);
    return client.send(command);
}

export async function listS3Objects(
    client: S3Client,
    params: S3ClientParams.ListObjectsRequest
): Promise<S3ClientResponse.ListObjectsOutput> {
    const command = new ListObjectsCommand(params);
    return client.send(command);
}

export async function putS3Object(
    client: S3Client,
    params: S3ClientParams.PutObjectRequest
): Promise<S3ClientResponse.PutObjectOutput> {
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

export async function deleteS3Objects(
    client: S3Client,
    params: S3ClientParams.DeleteObjectsRequest
): Promise<S3ClientResponse.DeleteObjectsOutput> {
    const command = new DeleteObjectsCommand(params);
    return client.send(command);
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
    } catch (error) {
        const { httpStatusCode } = (error as HeadBucketCommandOutput).$metadata;
        if (httpStatusCode === 404) {
            return false;
        }
        if (httpStatusCode === 403) {
            throw new TSError(`User does not have access to bucket "${params.Bucket}"`, { statusCode: 403 });
        }
        throw error;
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
