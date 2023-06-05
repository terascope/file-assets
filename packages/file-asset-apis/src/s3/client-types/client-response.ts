import { _Error, S3ServiceException } from '@aws-sdk/client-s3';

export type S3Error = _Error;
export type S3ErrorExceptions = S3ServiceException;

export {
    GetObjectOutput,
    ListObjectsOutput,
    ListObjectsV2Output,
    PutObjectOutput,
    PutObjectTaggingOutput,
    DeleteObjectOutput,
    DeleteObjectsOutput,
    ListBucketsOutput,
    CreateBucketOutput,
    CompletedPart,
} from '@aws-sdk/client-s3';
