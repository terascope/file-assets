import fs from 'fs-extra';
import { Agent, AgentOptions as HttpsAgentOptions } from 'https';
import { S3Client, S3ClientConfig } from '@aws-sdk/client-s3';
import { NodeHttpHandler, NodeHttpHandlerOptions } from '@smithy/node-http-handler';
import { debugLogger } from '@terascope/utils';

export interface S3ConnectionConfig {
    endpoint: string,
    region: string,
    forcePathStyle?: boolean,
    bucketEndpoint?: boolean,
    accessKeyId?: string
    secretAccessKey?: string,
    maxRetries?: number,
    sslEnabled?: boolean,
    certLocation?: string,
}

export interface S3ClientCredentials {
    accessKeyId: string;
    secretAccessKey: string;
}

export async function createS3Client(
    connectionConfig: S3ConnectionConfig,
    logger = debugLogger('s3-client')
): Promise<S3Client> {
    logger.info(`Using S3 endpoint: ${connectionConfig.endpoint}`);

    const clientConfig = await genS3ClientConfig(connectionConfig);

    // The aws v3 client logs every request and its metadata, it is too intrusive
    // so should only be used in trace mode otherwise it will log the body request
    // which has heavy performance implications
    if (logger.level() === 10) {
        clientConfig.logger = logger;
    }
    logger.debug(`createS3Client finalConfig:\n${JSON.stringify(clientConfig, null, 2)}`);

    return new S3Client(clientConfig);
}

/**
 * Given the terafoundation S3 connection configuration, generate the
 * AWS S3 client configuration object
 * @param {S3ConnectionConfig} connectionConfig terafoundation S3 connection configuration object
 * @returns {Promise<S3ClientConfig>} AWS S3 client configuration object
*/
export async function genS3ClientConfig(
    connectionConfig: S3ConnectionConfig
): Promise<S3ClientConfig> {
    const clientConfig: S3ClientConfig = {
        endpoint: connectionConfig.endpoint,
        region: connectionConfig.region,
        forcePathStyle: connectionConfig.forcePathStyle,
        bucketEndpoint: connectionConfig.bucketEndpoint
    };

    if (connectionConfig.maxRetries) {
        clientConfig.maxAttempts = connectionConfig.maxRetries;
    }

    if (connectionConfig.sslEnabled) {
        if (!connectionConfig.endpoint.includes('https')) {
            throw new Error(`S3 endpoint ${connectionConfig.endpoint} must be https if sslEnabled`);
        }
        const httpOptions = await createHttpOptions(connectionConfig);
        const requestHandlerOptions = createRequestHandlerOptions(httpOptions);
        clientConfig.requestHandler = new NodeHttpHandler(requestHandlerOptions);
    }

    if (!connectionConfig.sslEnabled && connectionConfig.endpoint.includes('https')) {
        throw new Error(`S3 endpoint ${connectionConfig.endpoint} cannot be https if sslEnabled is false`);
    }

    clientConfig.credentials = createCredentialsObject(connectionConfig);

    return clientConfig;
}

/**
 * Given the terafoundation S3 connection configuration, return the appropriate httpOptions.
 * @param {S3ConnectionConfig} connectionConfig terafoundation S3 connection configuration object
 * @returns {Promise<HttpsAgentOptions>} Options used to make HttpsAgent
 */
export async function createHttpOptions(
    connectionConfig: S3ConnectionConfig
): Promise<HttpsAgentOptions> {
    // pull certLocation from env
    // https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/node-registering-certs.html
    // Instead of updating the client, we can just update the config before creating the client

    const certPath = connectionConfig.certLocation ?? '/etc/ssl/certs/ca-certificates.crt';
    const pathFound = await fs.exists(certPath);

    if (!pathFound) {
        throw new Error(
            `No cert path was found in config.certLocation: "${connectionConfig.certLocation}" or in default "/etc/ssl/certs/ca-certificates.crt" location`
        );
    }
    // Assumes all certs needed are in a single bundle
    const certs = await fs.readFile(certPath);

    return {
        rejectUnauthorized: true,
        ca: [certs]
    };
}

/**
 * Given HttpAgentOptions, create NodeHttpHandlerOptions
 * @param {S3ConnectionConfig} HttpsAgentOptions terafoundation S3 connection configuration object
 * @returns {NodeHttpHandlerOptions}
 */
export function createRequestHandlerOptions(
    httpOptions: HttpsAgentOptions
): NodeHttpHandlerOptions {
    return { httpsAgent: new Agent(httpOptions) };
}

/**
 * Given the terafoundation S3 connection configuration, create a credentials object
 * containing accessKeyId and secretAccessKey.
 * The S3 connection config uses the S3Client V2 style that is no longer supported.
 * @param {S3ConnectionConfig} connectionConfig terafoundation S3 connection configuration object
 * @returns {S3ClientCredentials}
 */
export function createCredentialsObject(
    connectionConfig: S3ConnectionConfig
): S3ClientCredentials {
    if (!connectionConfig.accessKeyId) {
        throw new Error('S3 accessKeyId must be defined in S3ConnectionConfig');
    }
    if (!connectionConfig.secretAccessKey) {
        throw new Error('S3 secretAccessKey must be defined in S3ConnectionConfig');
    }
    const { accessKeyId, secretAccessKey } = connectionConfig;
    return {
        accessKeyId,
        secretAccessKey
    };
}
