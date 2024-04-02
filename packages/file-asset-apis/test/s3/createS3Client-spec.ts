import 'jest-extended';
import path from 'path';
import {
    createCredentialsObject,
    createHttpOptions,
    createRequestHandlerOptions,
    genFinalS3ClientConfig,
} from '../../src/s3/createS3Client';

describe('createS3Client', () => {
    describe('genS3ClientConfig', () => {
        it('should generate config with requestHandler if sslEnabled is true', async () => {
            const startConfig = {
                endpoint: 'https://127.0.0.1:49000',
                accessKeyId: 'minioadmin',
                secretAccessKey: 'minioadmin',
                region: 'us-east-1',
                maxRetries: 3,
                sslEnabled: true,
                certLocation: path.join(__dirname, '../__fixtures__/cert/fakeCert.pem'),
                forcePathStyle: true,
                bucketEndpoint: false
            };

            const result = await genFinalS3ClientConfig(startConfig);
            expect(result).toEqual(
                {
                    endpoint: 'https://127.0.0.1:49000',
                    accessKeyId: 'minioadmin',
                    secretAccessKey: 'minioadmin',
                    region: 'us-east-1',
                    maxRetries: 3,
                    sslEnabled: true,
                    certLocation: path.join(__dirname, '../__fixtures__/cert/fakeCert.pem'),
                    forcePathStyle: true,
                    bucketEndpoint: false,
                    maxAttempts: 3,
                    requestHandler: {
                        metadata: { handlerProtocol: 'http/1.1' },
                        configProvider: expect.toBeObject(),
                        socketWarningTimestamp: 0
                    },
                    credentials: { accessKeyId: 'minioadmin', secretAccessKey: 'minioadmin' }
                });
        });

        it('should generate config without requestHandler if sslEnabled is false', async () => {
            const startConfig = {
                endpoint: 'http://127.0.0.1:49000',
                accessKeyId: 'minioadmin',
                secretAccessKey: 'minioadmin',
                region: 'us-east-1',
                maxRetries: 3,
                sslEnabled: false,
                certLocation: path.join(__dirname, '../__fixtures__/cert/fakeCert.pem'),
                forcePathStyle: true,
                bucketEndpoint: false
            };

            const result = await genFinalS3ClientConfig(startConfig);
            expect(result).toEqual(
                {
                    endpoint: 'http://127.0.0.1:49000',
                    accessKeyId: 'minioadmin',
                    secretAccessKey: 'minioadmin',
                    region: 'us-east-1',
                    maxRetries: 3,
                    sslEnabled: false,
                    certLocation: path.join(__dirname, '../__fixtures__/cert/fakeCert.pem'),
                    forcePathStyle: true,
                    bucketEndpoint: false,
                    maxAttempts: 3,
                    credentials: { accessKeyId: 'minioadmin', secretAccessKey: 'minioadmin' }
                });
        });

        it('should throw error if sslEnabled is false and endpoint is https', async () => {
            const startConfig = {
                endpoint: 'https://127.0.0.1:49000',
                accessKeyId: 'minioadmin',
                secretAccessKey: 'minioadmin',
                region: 'us-east-1',
                maxRetries: 3,
                sslEnabled: false,
                certLocation: path.join(__dirname, '../__fixtures__/cert/fakeCert.pem'),
                forcePathStyle: true,
                bucketEndpoint: false
            };

            await expect(() => genFinalS3ClientConfig(startConfig)).rejects.toThrow(`S3 endpoint ${startConfig.endpoint} cannot be https if sslEnabled is false`);
        });

        it('should throw error if sslEnabled is true and endpoint is http', async () => {
            const startConfig = {
                endpoint: 'http://127.0.0.1:49000',
                accessKeyId: 'minioadmin',
                secretAccessKey: 'minioadmin',
                region: 'us-east-1',
                maxRetries: 3,
                sslEnabled: true,
                certLocation: path.join(__dirname, '../__fixtures__/cert/fakeCert.pem'),
                forcePathStyle: true,
                bucketEndpoint: false
            };

            await expect(() => genFinalS3ClientConfig(startConfig))
                .rejects.toThrow(`S3 endpoint ${startConfig.endpoint} must be https if sslEnabled`);
        });

        it('should accept credentials object', async () => {
            const startConfig = {
                endpoint: 'https://127.0.0.1:49000',
                credentials: {
                    accessKeyId: 'minioadmin',
                    secretAccessKey: 'minioadmin'
                },
                region: 'us-east-1',
                maxRetries: 3,
                sslEnabled: true,
                certLocation: path.join(__dirname, '../__fixtures__/cert/fakeCert.pem'),
                forcePathStyle: true,
                bucketEndpoint: false
            };

            const result = await genFinalS3ClientConfig(startConfig);
            expect(result).toEqual(
                {
                    endpoint: 'https://127.0.0.1:49000',
                    region: 'us-east-1',
                    maxRetries: 3,
                    sslEnabled: true,
                    certLocation: path.join(__dirname, '../__fixtures__/cert/fakeCert.pem'),
                    forcePathStyle: true,
                    bucketEndpoint: false,
                    maxAttempts: 3,
                    requestHandler: {
                        metadata: { handlerProtocol: 'http/1.1' },
                        configProvider: expect.toBeObject(),
                        socketWarningTimestamp: 0
                    },
                    credentials: { accessKeyId: 'minioadmin', secretAccessKey: 'minioadmin' }
                });
        });
    });

    describe('createHttpOptions', () => {
        it('should throw an error if certLocation is an invalid path', async () => {
            const startConfig = {
                endpoint: 'https://127.0.0.1:49000',
                region: 'us-east-1',
                sslEnabled: true,
                certLocation: 'invalid/path/fakeCert.pem'
            };

            await expect(createHttpOptions(startConfig))
                .rejects.toThrow(`No cert path was found in config.certLocation: "${startConfig.certLocation}" or in default "/etc/ssl/certs/ca-certificates.crt" location`);
        });

        it('should return an httpOptions with certs if sslEnabled is true', async () => {
            const startConfig = {
                endpoint: 'https://127.0.0.1:49000',
                region: 'us-east-1',
                sslEnabled: true,
                certLocation: path.join(__dirname, '../__fixtures__/cert/fakeCert.pem')
            };

            const result = await createHttpOptions(startConfig);
            expect(result).toEqual({
                rejectUnauthorized: true,
                ca: expect.toBeArray()
            });
        });
    });

    describe('createHandlerOptions', () => {
        it('should return requestHandlerOptions with an Agent', async () => {
            const httpOptions = {
                rejectUnauthorized: true,
                ca: ['some buffer']
            };

            const result = createRequestHandlerOptions(httpOptions);
            expect(result).toEqual({
                httpsAgent: expect.objectContaining({
                    options: {
                        rejectUnauthorized: true,
                        ca: ['some buffer'],
                        noDelay: true,
                        path: null
                    }
                })
            });
        });
    });

    describe('moveCredentialsIntoObject', () => {
        it('should throw error if secretAccessKey not defined', () => {
            const startConfig = {
                endpoint: 'https://127.0.0.1:49000',
                region: 'us-east-1',
                accessKeyId: 'minioadmin',
            };
            expect(() => createCredentialsObject(startConfig)).toThrow('S3 secretAccessKey must be defined in S3ClientConfig');
        });

        it('should throw error if accessKeyId not defined', () => {
            const startConfig = {
                endpoint: 'https://127.0.0.1:49000',
                region: 'us-east-1',
                secretAccessKey: 'minioadmin',
            };
            expect(() => createCredentialsObject(startConfig)).toThrow('S3 accessKeyId must be defined in S3ClientConfig');
        });

        it('should return config with credentials object', () => {
            const startConfig = {
                endpoint: 'https://127.0.0.1:49000',
                region: 'us-east-1',
                accessKeyId: 'minioadmin',
                secretAccessKey: 'minioadmin',
            };

            const result = createCredentialsObject(startConfig);
            expect(result).toEqual({ accessKeyId: 'minioadmin', secretAccessKey: 'minioadmin' });
        });
    });
});
