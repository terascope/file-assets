import { isError } from '@terascope/utils';
import {
    S3Client, S3ClientResponse, deleteAllS3Objects,
    deleteS3Bucket,
    ChunkedFileSenderConfig, Compression, Formatter,
    Compressor, createFileName, putS3Object, createS3Client
} from '../../src';

const {
    MINIO_HOST = 'http://127.0.0.1:9000',
    MINIO_ACCESS_KEY = 'minioadmin',
    MINIO_SECRET_KEY = 'minioadmin',
} = process.env;

export { MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY };

export async function makeClient() {
    return createS3Client({
        endpoint: MINIO_HOST,
        accessKeyId: MINIO_ACCESS_KEY,
        secretAccessKey: MINIO_SECRET_KEY,
        maxRetries: 4,
        forcePathStyle: true,
        sslEnabled: false,
        region: 'us-east-1'
    });
}

export async function cleanupBucket(
    client: S3Client, bucket: string
): Promise<void> {
    try {
        await deleteAllS3Objects(client, { Bucket: bucket });
        await deleteS3Bucket(client, { Bucket: bucket });
    } catch (err: any) {
        if (isError(err) && (err as S3ClientResponse.S3Error).Code === 'NoSuchBucket') {
            return;
        }
        throw err;
    }
}

export async function getBodyFromResults(
    results: S3ClientResponse.GetObjectCommandOutput
): Promise<Buffer> {
    if (!results.Body) {
        throw new Error('Missing body from s3 results');
    }
    const data = await results.Body.transformToByteArray();

    return Buffer.from(data);
}

export interface UploadConfig extends Partial<ChunkedFileSenderConfig> {
    sliceCount: number,
    bucket: string,
}

export async function upload(
    client: S3Client, config: UploadConfig, data: Record<string, any>[]
): Promise<string> {
    const {
        format, compression = Compression.none,
        extension, sliceCount, path, bucket, id
    } = config;

    if (format == null) throw new Error('format must be provided');
    if (path == null) throw new Error('path must be provided');
    if (id == null) throw new Error('id must be provided');

    const formatter = new Formatter(config as ChunkedFileSenderConfig);
    const compressionFormatter = new Compressor(compression);

    const formattedData = formatter.format(data);
    const finalData = await compressionFormatter.compress(formattedData) as any;

    const fileName = createFileName(path, {
        filePerSlice: true,
        id,
        extension,
        format,
        sliceCount
    });

    const params = {
        Bucket: bucket,
        Key: fileName,
        Body: finalData
    };

    await putS3Object(client, params);

    return fileName;
}
