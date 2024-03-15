import {
    ListObjectsV2Request,
    GetObjectRequest,
    ListObjectsV2Output,
    GetObjectCommandOutput,
    S3Client
} from '@aws-sdk/client-s3';

export type S3RetryParams = ListObjectsV2Request | GetObjectRequest;
export type S3RetryResponse = ListObjectsV2Output | GetObjectCommandOutput;

export type RetryArgs = {
    client: S3Client,
    func: (client: S3Client, params: any) => Promise<S3RetryResponse>,
    params: S3RetryParams
};

export type ListObjectsWithRetry = {
    client: S3Client,
    func: (client: S3Client, params: ListObjectsV2Request) => Promise<ListObjectsV2Output>,
    params: ListObjectsV2Request
};

export type GetObjectWithRetry = {
    client: S3Client,
    func: (client: S3Client, params: GetObjectRequest) => Promise<GetObjectCommandOutput>,
    params: GetObjectRequest
};
