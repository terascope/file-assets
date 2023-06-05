import {
    AnyObject, debugLogger, isNil,
    isString, DataEntity, isError
} from '@terascope/job-components';
import {
    S3Fetcher, S3Sender, FileSlice, Format,
    deleteS3Objects, listS3Objects, deleteS3Bucket,
    ChunkedFileSenderConfig, ReaderConfig, createS3Client,
    S3Client, S3ClientResponse
} from '@terascope/file-asset-apis';
import * as s3Config from './config';

const logger = debugLogger('s3_tests');

export async function makeClient() {
    return createS3Client({
        endpoint: s3Config.MINIO_HOST,
        credentials: {
            accessKeyId: s3Config.MINIO_ACCESS_KEY,
            secretAccessKey: s3Config.MINIO_SECRET_KEY,
        },
        maxAttempts: 4,
        forcePathStyle: true,
        sslEnabled: false,
        region: 'us-east-1'
    });
}

export const testWorkerId = 'test-id';

const defaultSenderConfigs: Partial<ChunkedFileSenderConfig> = {
    concurrency: 10,
    format: Format.ldjson,
    id: testWorkerId,
    file_per_slice: true
};

const defaultReaderConfig: Partial<ReaderConfig> = {
    size: 10000
};

export async function fetch(
    client: S3Client, config: Partial<ReaderConfig>, slice: FileSlice
): Promise<string> {
    if (isNil(config.path) || !isString(config.path)) throw new Error('config must include parameter path');

    const fetchConfig = Object.assign({}, defaultReaderConfig, config) as ReaderConfig;
    const api = new S3Fetcher(client, fetchConfig, logger);

    // @ts-expect-error
    return api.fetch(slice);
}

export async function upload(
    client: S3Client, config: AnyObject, data: DataEntity[]
): Promise<number> {
    if (isNil(config.bucket) || !isString(config.bucket)) throw new Error('config must include parameter bucket');
    if (isNil(config.path) || !isString(config.path)) throw new Error('config must include parameter path');

    const senderConfig = Object.assign({}, defaultSenderConfigs, config) as ChunkedFileSenderConfig;
    const api = new S3Sender(client, senderConfig, logger);

    await api.ensureBucket();

    return api.send(data);
}

export async function cleanupBucket(
    client: S3Client, bucket: string
): Promise<void> {
    try {
        const request = await listS3Objects(client, {
            Bucket: bucket,
        });

        const objects = request.Contents?.map((obj) => ({ Key: obj.Key! }));
        await deleteS3Objects(client, { Bucket: bucket, Delete: { Objects: objects } });

        await deleteS3Bucket(client, { Bucket: bucket });
    } catch (err: any) {
        if (isError(err) && (err as S3ClientResponse.S3Error).Code === 'NoSuchBucket') {
            return;
        }
        throw err;
    }
}

export async function getBodyFromResults(
    results: S3ClientResponse.GetObjectOutput
): Promise<Buffer> {
    if (!results.Body) {
        throw new Error('Missing body from s3 results');
    }
    // @ts-expect-error, their types do not list added apis
    const data = await results.Body.transformToByteArray();
    return Buffer.from(data);
}
