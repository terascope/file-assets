import {
    ListObjectsV2Request,
    ListObjectsV2CommandOutput,
    GetObjectRequest,
    GetObjectCommandOutput,
    PutObjectCommandOutput,
    PutObjectRequest,
    DeleteObjectRequest,
    DeleteObjectCommandOutput,
    HeadObjectRequest,
    HeadBucketRequest,
    S3Client
} from '@aws-sdk/client-s3';

export type S3RetryParams
    = ListObjectsV2Request | GetObjectRequest | DeleteObjectRequest
        | PutObjectRequest | HeadObjectRequest | HeadBucketRequest;
export type S3RetryResponse
    = ListObjectsV2CommandOutput | GetObjectCommandOutput
        | PutObjectCommandOutput | DeleteObjectCommandOutput | boolean;

export type RetryArgs = {
    client: S3Client;
    func: (client: S3Client, params: any) => Promise<S3RetryResponse>;
    params: S3RetryParams;
};

export type ListObjectsWithRetry = {
    client: S3Client;
    func: (client: S3Client, params: ListObjectsV2Request) => Promise<ListObjectsV2CommandOutput>;
    params: ListObjectsV2Request;
};

export type GetObjectWithRetry = {
    client: S3Client;
    func: (client: S3Client, params: GetObjectRequest) => Promise<GetObjectCommandOutput>;
    params: GetObjectRequest;
};

export type PutObjectWithRetry = {
    client: S3Client;
    func: (client: S3Client, params: PutObjectRequest) => Promise<PutObjectCommandOutput>;
    params: PutObjectRequest;
};

export type DeleteObjectWithRetry = {
    client: S3Client;
    func: (client: S3Client, params: DeleteObjectRequest) => Promise<DeleteObjectCommandOutput>;
    params: DeleteObjectRequest;
};

export type DoesObjectExistWithRetry = {
    client: S3Client;
    func: (client: S3Client, params: HeadObjectRequest) => Promise<boolean>;
    params: HeadObjectRequest;
};

export type DoesBucketExistWithRetry = {
    client: S3Client;
    func: (client: S3Client, params: HeadBucketRequest) => Promise<boolean>;
    params: HeadBucketRequest;
};
