import {
    S3Client, GetObjectRequest, GetObjectCommand,
    GetObjectOutput, ListObjectsRequest, ListObjectsOutput,
    ListObjectsCommand, PutObjectRequest, PutObjectOutput,
    PutObjectCommand, DeleteObjectRequest, DeleteObjectOutput,
    DeleteObjectCommand, DeleteBucketRequest, DeleteBucketCommand,
    HeadBucketRequest, HeadBucketCommand, ListBucketsCommand,
    ListBucketsOutput, CreateBucketRequest, CreateBucketOutput,
    CreateBucketCommand, CreateMultipartUploadCommand, UploadPartRequest,
    CompletedPart, UploadPartCommand, CompleteMultipartUploadRequest,
    CompleteMultipartUploadCommand, AbortMultipartUploadRequest,
    AbortMultipartUploadCommand
} from '@aws-sdk/client-s3';

export async function getS3Object(
    client: S3Client,
    params: GetObjectRequest
): Promise<GetObjectOutput> {
    const command = new GetObjectCommand(params);
    return client.send(command);
}

export async function listS3Objects(
    client: S3Client,
    params: ListObjectsRequest
): Promise<ListObjectsOutput> {
    const command = new ListObjectsCommand(params);
    return client.send(command);
}

export async function putS3Object(
    client: S3Client,
    params: PutObjectRequest
): Promise<PutObjectOutput> {
    const command = new PutObjectCommand(params);
    return client.send(command);
}

export async function deleteS3Object(
    client: S3Client,
    params: DeleteObjectRequest
): Promise<DeleteObjectOutput> {
    const command = new DeleteObjectCommand(params);
    return client.send(command);
}

export async function deleteS3Bucket(
    client: S3Client,
    params: DeleteBucketRequest
): Promise<void> {
    const command = new DeleteBucketCommand(params);
    await client.send(command);
}

export async function headS3Bucket(
    client: S3Client,
    params: HeadBucketRequest
): Promise<void> {
    const command = new HeadBucketCommand(params);
    await client.send(command);
}

export async function listS3Buckets(
    client: S3Client,
): Promise<ListBucketsOutput> {
    const command = new ListBucketsCommand({});
    return client.send(command);
}

export async function createS3Bucket(
    client: S3Client,
    params: CreateBucketRequest
): Promise<CreateBucketOutput> {
    const command = new CreateBucketCommand(params);
    return client.send(command);
}

export async function createS3MultipartUpload(
    client: S3Client, Bucket: string, Key: string
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
    client: S3Client, params: UploadPartRequest
): Promise<CompletedPart> {
    const command = new UploadPartCommand(params);
    return client.send(command);
}

export async function finalizeS3Multipart(
    client: S3Client, params: CompleteMultipartUploadRequest
): Promise<void> {
    const command = new CompleteMultipartUploadCommand(params);
    await client.send(command);
}

export async function abortS3Multipart(
    client: S3Client, params: AbortMultipartUploadRequest
): Promise<void> {
    const command = new AbortMultipartUploadCommand(params);
    await client.send(command);
}
