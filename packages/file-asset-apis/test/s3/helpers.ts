import S3 from 'aws-sdk/clients/s3';
import {
    listS3Objects,
    deleteS3Object,
    deleteS3Bucket,
    ChunkedFileSenderConfig,
    Compression,
    Formatter,
    Compressor,
    createFileName,
    S3PutConfig,
    putS3Object
} from '../../src';

const {
    ENDPOINT = 'http://127.0.0.1:9000',
    ACCESS_KEY = 'minioadmin',
    SECRET_KEY = 'minioadmin',
} = process.env;

export function makeClient(): S3 {
    return new S3({
        endpoint: ENDPOINT,
        accessKeyId: ACCESS_KEY,
        secretAccessKey: SECRET_KEY,
        maxRetries: 3,
        maxRedirects: 10,
        s3ForcePathStyle: true,
        sslEnabled: false,
        region: 'us-east-1'
    });
}

export async function cleanupBucket(
    client: S3, bucket: string
): Promise<void> {
    let request: S3.ListObjectsOutput;
    try {
        request = await listS3Objects(client, {
            Bucket: bucket,
        });
    } catch (err) {
        if (err.code === 'NoSuchBucket') return;
        throw err;
    }

    const promises = request.Contents?.map((obj) => deleteS3Object(client, {
        Bucket: bucket, Key: obj.Key!
    }));

    await Promise.all(promises ?? []);

    await deleteS3Bucket(client, { Bucket: bucket });
}

export function getBodyFromResults(results: S3.GetObjectOutput): Buffer {
    if (!results.Body) {
        throw new Error('Missing body from s3 results');
    }
    return Buffer.isBuffer(results.Body)
        ? results.Body
        : Buffer.from(results.Body as string);
}

export interface UploadConfig extends Partial<ChunkedFileSenderConfig> {
    sliceCount: number,
    bucket: string,
}

export async function upload(
    client: S3, config: UploadConfig, data: Record<string, any>[]
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
    const finalData = await compressionFormatter.compress(formattedData);

    const fileName = createFileName(path, {
        filePerSlice: true,
        id,
        extension,
        format,
        sliceCount
    });

    const params: S3PutConfig = {
        Bucket: bucket,
        Key: fileName,
        Body: finalData
    };

    await putS3Object(client, params);

    return fileName;
}

export { ENDPOINT, ACCESS_KEY, SECRET_KEY };
