import { _Error, S3ServiceException } from '@aws-sdk/client-s3';

export type S3Error = _Error;
export type S3ErrorExceptions = S3ServiceException;

export {
    BucketAlreadyExists,
    BucketAlreadyOwnedByYou,
    NoSuchKey,
    ListObjectsV2Output,
    PutObjectTaggingOutput,
    DeleteObjectOutput,
    DeleteObjectsOutput,
    ListBucketsOutput,
    CreateBucketOutput,
    GetObjectCommandOutput,
    CompletedPart,
    PutObjectCommandOutput
} from '@aws-sdk/client-s3';
