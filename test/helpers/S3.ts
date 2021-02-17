import {
    AnyObject,
    debugLogger,
    isNil,
    isString,
    DataEntity
} from '@terascope/job-components';
import S3 from 'aws-sdk/clients/s3';
import { promisifyAll, defer } from 'bluebird';
import {
    S3Reader, S3Sender, SlicedFileResults, Format, Compression
} from '@terascope/file-asset-apis';
import * as s3Config from './config';

const logger = debugLogger('s3_tests');

export function makeClient(): AnyObject {
    const config = {
        defer: () => defer,
        endpoint: s3Config.ENDPOINT,
        accessKeyId: s3Config.ACCESS_KEY,
        secretAccessKey: s3Config.SECRET_KEY,
        maxRetries: 3,
        maxRedirects: 10,
        s3ForcePathStyle: true,
        sslEnabled: false,
        region: 'us-east-1'
    };
    const s3Client = new S3(config);
    const client = promisifyAll(s3Client, { suffix: '_Async' });

    return client;
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
    workerId: testWorkerId
};

export async function fetch(
    client: AnyObject, config: AnyObject, slice: SlicedFileResults
): Promise<string> {
    if (isNil(config.bucket) || !isString(config.bucket)) throw new Error('config must include parameter bucket');
    // TODO: fix this
    const fetchConfig = Object.assign({}, defaultConfigs, config) as any;
    const api = new S3Reader(client, fetchConfig, logger);
    // @ts-expect-error
    return api.fetch(slice);
}

export async function upload(
    client: AnyObject, config: AnyObject, data: DataEntity[]
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
    client: AnyObject, bucket: string
): Promise<void> {
    let request: AnyObject;
    try {
        request = await client.listObjects_Async({ Bucket: bucket });
    } catch (err) {
        if (err.code === 'NoSuchBucket') return;
        throw err;
    }

    const promises = request!.Contents.map((obj: AnyObject) => {
        const params = { Bucket: bucket, Key: obj.Key };
        return client.deleteObject_Async(params);
    });

    await Promise.all(promises);

    await client.deleteBucket_Async({ Bucket: bucket });
}
