import 'jest-extended';
import path from 'path';
import {
    addCertsIfSSLEnabled,
    addRequestHandler,
    createRequestHandlerOptions,
    genS3ClientConfig,
    mapMaxRetriesToMaxAttempts,
    moveCredentialsIntoObject,
    removeExtendedConfigKeys
} from '../../src/s3/createS3Client';

describe('createS3Client', () => {
    describe('genS3ClientConfig', () => {
        it('should generate proper config', async () => {
            const startConfig = {
                endpoint: 'https://127.0.0.1:49000',
                accessKeyId: 'minioadmin',
                secretAccessKey: 'minioadmin',
                handlerOptions: { requestTimeout: 30000 },
                region: 'us-east-1',
                maxRetries: 3,
                maxRedirects: 10,
                sslEnabled: true,
                certLocation: path.join(__dirname, '../__fixtures__/cert/fakeCert.pem'),
                forcePathStyle: true,
                bucketEndpoint: false
            };

            const result = await genS3ClientConfig(startConfig);
            expect(result).toEqual(
                {
                    endpoint: 'https://127.0.0.1:49000',
                    region: 'us-east-1',
                    maxAttempts: 3,
                    maxRedirects: 10,
                    forcePathStyle: true,
                    bucketEndpoint: false,
                    requestHandler: {
                        metadata: { handlerProtocol: 'http/1.1' },
                        configProvider: expect.toBeObject(),
                        socketWarningTimestamp: 0
                    },
                    credentials: { accessKeyId: 'minioadmin', secretAccessKey: 'minioadmin' }
                });
        });
    });

    describe('addCertsIfSSLEnabled', () => {
        it('should throw an error if sslEnabled is true and certLocation is an invalid path', async () => {
            const startConfig = {
                sslEnabled: true,
                certLocation: 'invalid/path/fakeCert.pem'
            };

            await expect(addCertsIfSSLEnabled(startConfig))
                .rejects.toThrow(`No cert path was found in config.certLocation: "${startConfig.certLocation}" or in default "/etc/ssl/certs/ca-certificates.crt" location`);
        });

        it('should return config with empty httpOptions if key did not exist and sslEnabled is false', async () => {
            const startConfig = {
                sslEnabled: false,
                certLocation: path.join(__dirname, '../__fixtures__/cert/fakeCert.pem'),
            };

            const endConfig = {
                sslEnabled: false,
                certLocation: path.join(__dirname, '../__fixtures__/cert/fakeCert.pem'),
                httpOptions: {}
            };

            const result = await addCertsIfSSLEnabled(startConfig);
            expect(result).toEqual(endConfig);
        });

        it('should not change httpOptions if sslEnabled is false', async () => {
            const startConfig = {
                sslEnabled: false,
                certLocation: path.join(__dirname, '../__fixtures__/cert/fakeCert.pem'),
                httpOptions: { maxCachedSessions: 3 }
            };

            const result = await addCertsIfSSLEnabled(startConfig);
            expect(result).toEqual(startConfig);
        });

        it('should return a config with updated httpOptions if sslEnabled is true', async () => {
            const startConfig = {
                sslEnabled: true,
                certLocation: path.join(__dirname, '../__fixtures__/cert/fakeCert.pem'),
                httpOptions: { maxCachedSessions: 3 }
            };

            const finalConfig = {
                sslEnabled: true,
                certLocation: path.join(__dirname, '../__fixtures__/cert/fakeCert.pem'),
                httpOptions: {
                    maxCachedSessions: 3,
                    rejectUnauthorized: true,
                    ca: expect.toBeArray()
                }
            };

            const result = await addCertsIfSSLEnabled(startConfig);
            expect(result).toEqual(finalConfig);
        });
    });

    describe('createHandlerOptions', () => {
        it('should return an empty object if there are no httpOptions or handlerOptions', async () => {
            const startConfig = {
                handlerOptions: {},
                httpOptions: {}
            };

            const result = createRequestHandlerOptions(startConfig);
            expect(result).toEqual({});
        });

        it('should return requestHandlerOptions with requestTimeout and connectionTimeout if handlerOptions specified', async () => {
            const startConfig = {
                handlerOptions: {
                    requestTimeout: 30000,
                    connectionTimeout: 30000
                },
            };

            const result = createRequestHandlerOptions(startConfig);
            expect(result).toEqual({
                requestTimeout: 30000,
                connectionTimeout: 30000
            });
        });

        it('should return requestHandlerOptions with an Agent if httpOptions specified', async () => {
            const startConfig = {
                httpOptions: {
                    rejectUnauthorized: true,
                    ca: ['some buffer']
                }
            };

            const result = createRequestHandlerOptions(startConfig);
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

        it('should return requestHandlerOptions with requestTimeout, connectionTimeout, and options if handlerOptions and httpOptions specified', async () => {
            const startConfig = {
                handlerOptions: {
                    requestTimeout: 30000,
                    connectionTimeout: 30000
                },
                httpOptions: {
                    rejectUnauthorized: true,
                    ca: ['some buffer']
                }
            };

            const result = createRequestHandlerOptions(startConfig);
            expect(result).toEqual({
                requestTimeout: 30000,
                connectionTimeout: 30000,
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

    describe('addRequestHandler', () => {
        it('should return original config if requestHandlerOptions is an empty object', async () => {
            const startConfig = {
                endpoint: 'https://127.0.0.1:49000',
                accessKeyId: 'minioadmin',
                secretAccessKey: 'minioadmin',
                region: 'us-east-1',
                sslEnabled: true,
                certLocation: path.join(__dirname, '../__fixtures__/cert/fakeCert.pem'),
            };

            const result = addRequestHandler(startConfig, {});
            expect(result).toEqual(startConfig);
        });

        it('should return config with requestHandler if requestHandlerOptions has values', async () => {
            const requestHandlerOptions = {
                requestTimeout: 30000,
                connectionTimeout: 30000,
                rejectUnauthorized: true,
                ca: []
            };

            const startConfig = {
                endpoint: 'https://127.0.0.1:49000',
                accessKeyId: 'minioadmin',
                secretAccessKey: 'minioadmin',
            };

            const endConfig = {
                endpoint: 'https://127.0.0.1:49000',
                accessKeyId: 'minioadmin',
                secretAccessKey: 'minioadmin',
                requestHandler: {
                    metadata: { handlerProtocol: 'http/1.1' },
                    configProvider: expect.toBeObject(),
                    socketWarningTimestamp: 0
                },
            };

            const result = addRequestHandler(startConfig, requestHandlerOptions);
            expect(result).toEqual(endConfig);
        });
    });

    describe('moveCredentialsIntoObject', () => {
        it('should return original config if credentials key defined', () => {
            const startConfig = {
                accessKeyId: 'minioadmin',
                secretAccessKey: 'minioadmin',
                credentials: { accessKeyId: 'minioadmin', secretAccessKey: 'minioadmin' }
            };
            const result = moveCredentialsIntoObject(startConfig);
            expect(result).toEqual(startConfig);
        });

        it('should return original config if credentials secretAccessKey not defined', () => {
            const startConfig = {
                accessKeyId: 'minioadmin',
            };
            const result = moveCredentialsIntoObject(startConfig);
            expect(result).toEqual(startConfig);
        });

        it('should return original config if credentials accessKeyId not defined', () => {
            const startConfig = {
                secretAccessKey: 'minioadmin',
            };
            const result = moveCredentialsIntoObject(startConfig);
            expect(result).toEqual(startConfig);
        });

        it('should return config with credentials object', () => {
            const startConfig = {
                accessKeyId: 'minioadmin',
                secretAccessKey: 'minioadmin',
            };

            const endConfig = {
                credentials: { accessKeyId: 'minioadmin', secretAccessKey: 'minioadmin' }
            };
            const result = moveCredentialsIntoObject(startConfig);
            expect(result).toEqual(endConfig);
        });
    });

    describe('mapMaxRetriesToMaxAttempts', () => {
        it('should remove maxRetries if both maxRetries and maxAttempts defined', async () => {
            const startConfig = {
                maxRetries: 3,
                maxAttempts: 4,
            };

            const endConfig = {
                maxAttempts: 4,
            };

            const result = mapMaxRetriesToMaxAttempts(startConfig);
            expect(result).toEqual(endConfig);
        });

        it('should replace maxRetries key with maxAttempts', async () => {
            const startConfig = {
                endpoint: 'https://127.0.0.1:49000',
                region: 'us-east-1',
                maxRetries: 3
            };

            const endConfig = {
                endpoint: 'https://127.0.0.1:49000',
                region: 'us-east-1',
                maxAttempts: 3
            };
            const result = mapMaxRetriesToMaxAttempts(startConfig);
            expect(result).toEqual(endConfig);
        });
    });

    describe('removeExtendedConfigKeys', () => {
        it('should remove keys that are invalid for baseS3Config', async () => {
            const startConfig = {
                sslEnabled: true,
                certLocation: path.join(__dirname, '../__fixtures__/cert/fakeCert.pem'),
                httpOptions: {
                    rejectUnauthorized: true,
                    ca: []
                },
                handlerOptions: {
                    requestTimeout: 30000,
                    connectionTimeout: 30000
                }
            };

            const endConfig = {
            };
            const result = removeExtendedConfigKeys(startConfig);
            expect(result).toEqual(endConfig);
        });
    });
});
