import S3 from 'aws-sdk/clients/s3';

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

export { ENDPOINT, ACCESS_KEY, SECRET_KEY };
