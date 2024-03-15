import { _Error, S3ServiceException } from '@aws-sdk/client-s3';

export type S3Error = _Error;
export type S3ErrorExceptions = S3ServiceException;

export {
    ListObjectsV2Output,
    PutObjectOutput,
    PutObjectTaggingOutput,
    DeleteObjectOutput,
    DeleteObjectsOutput,
    ListBucketsOutput,
    CreateBucketOutput,
    GetObjectCommandOutput,
    CompletedPart,
} from '@aws-sdk/client-s3';
