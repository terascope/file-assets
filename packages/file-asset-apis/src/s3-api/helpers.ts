import type S3 from 'aws-sdk/clients/s3';

export async function getS3Object(
    client: S3,
    params: S3.GetObjectRequest
): Promise<S3.GetObjectOutput> {
    return new Promise<S3.GetObjectOutput>((resolve, reject) => {
        client.getObject(params, (err, data) => {
            if (err) reject(err);
            else resolve(data);
        });
    });
}

export async function listS3Objects(
    client: S3,
    params: S3.ListObjectsRequest
): Promise<S3.ListObjectsOutput> {
    return new Promise<S3.ListObjectsOutput>((resolve, reject) => {
        client.listObjects(params, (err, data) => {
            if (err) reject(err);
            else resolve(data);
        });
    });
}

export async function putS3Object(
    client: S3,
    params: S3.PutObjectRequest
): Promise<S3.PutObjectOutput> {
    return new Promise<S3.PutObjectOutput>((resolve, reject) => {
        client.putObject(params, (err, data) => {
            if (err) reject(err);
            else resolve(data);
        });
    });
}

export async function deleteS3Object(
    client: S3,
    params: S3.DeleteObjectRequest
): Promise<S3.DeleteObjectOutput> {
    return new Promise<S3.DeleteObjectOutput>((resolve, reject) => {
        client.deleteObject(params, (err, data) => {
            if (err) reject(err);
            else resolve(data);
        });
    });
}

export async function deleteS3Bucket(
    client: S3,
    params: S3.DeleteBucketRequest
): Promise<void> {
    return new Promise<void>((resolve, reject) => {
        client.deleteBucket(params, (err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

export async function headS3Bucket(
    client: S3,
    params: S3.HeadBucketRequest
): Promise<void> {
    return new Promise<void>((resolve, reject) => {
        client.headBucket(params, (err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

export async function listS3Buckets(
    client: S3,
): Promise<S3.ListBucketsOutput> {
    return new Promise<S3.ListBucketsOutput>((resolve, reject) => {
        client.listBuckets((err, data) => {
            if (err) reject(err);
            else resolve(data);
        });
    });
}

export async function createS3Bucket(
    client: S3,
    params: S3.CreateBucketRequest
): Promise<S3.CreateBucketOutput> {
    return new Promise<S3.CreateBucketOutput>((resolve, reject) => {
        client.createBucket(params, (err, data) => {
            if (err) reject(err);
            else resolve(data);
        });
    });
}
