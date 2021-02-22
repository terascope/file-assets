import {
    AnyObject,
    debugLogger,
    isNil,
    isString,
    DataEntity
} from '@terascope/job-components';
import S3 from 'aws-sdk/clients/s3';
import {
    S3Reader, S3Sender, FileSlice, Format, Compression,
    deleteS3Object, listS3Objects, deleteS3Bucket
} from '@terascope/file-asset-apis';
import * as s3Config from './config';

const logger = debugLogger('s3_tests');

export function makeClient(): S3 {
    return new S3({
        endpoint: s3Config.ENDPOINT,
        accessKeyId: s3Config.ACCESS_KEY,
        secretAccessKey: s3Config.SECRET_KEY,
        maxRetries: 3,
        maxRedirects: 10,
        s3ForcePathStyle: true,
        sslEnabled: false,
        region: 'us-east-1'
    });
}

export const testWorkerId = 'test-id';

const defaultConfigs = {
    concurrency: 10,
    format: Format.ldjson,
    line_delimiter: '\n',
    field_delimiter: ',',
    compression: Compression.none,
    remove_header: true,
    ignore_empty: true,
    size: 10000000,
    fields: [],
    _dead_letter_action: 'throw',
    worker_id: testWorkerId
};

export async function fetch(
    client: S3, config: AnyObject, slice: FileSlice
): Promise<string> {
    if (isNil(config.bucket) || !isString(config.bucket)) throw new Error('config must include parameter bucket');
    // TODO: fix this
    const fetchConfig = Object.assign({}, defaultConfigs, config) as any;
    const api = new S3Reader(client, fetchConfig, logger);
    // @ts-expect-error
    return api.fetch(slice);
}

export async function upload(
    client: S3, config: AnyObject, data: DataEntity[]
): Promise<void> {
    if (isNil(config.bucket) || !isString(config.bucket)) throw new Error('config must include parameter bucket');
    if (isNil(config.path) || !isString(config.path)) throw new Error('config must include parameter path');

    // TODO: fix this
    const senderConfig = Object.assign({}, defaultConfigs, config) as any;
    const api = new S3Sender(client, senderConfig, logger);

    await api.ensureBucket(config.bucket);

    return api.send(data);
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
