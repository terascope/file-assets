import { _Error, S3ServiceException } from '@aws-sdk/client-s3';

export type S3Error = _Error;
export type S3ErrorExceptions = S3ServiceException;

export type {
    BucketAlreadyExists,
    BucketAlreadyOwnedByYou,
    NoSuchKey,
    CompletedPart,
    ListObjectsV2CommandOutput,
    PutObjectTaggingCommandOutput,
    DeleteObjectCommandOutput,
    DeleteObjectsCommandOutput,
    ListBucketsCommandOutput,
    CreateBucketCommandOutput,
    GetObjectCommandOutput,
    PutObjectCommandOutput
} from '@aws-sdk/client-s3';
