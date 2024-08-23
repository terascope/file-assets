import fs from 'node:fs';
import tls from 'node:tls';
import { Agent, AgentOptions as HttpsAgentOptions } from 'node:https';
import { S3Client as BaseClient } from '@aws-sdk/client-s3';
import { NodeHttpHandler, NodeHttpHandlerOptions } from '@smithy/node-http-handler';
import type { S3ClientConfig as BaseConfig } from '@aws-sdk/client-s3';
import {
    debugLogger
} from '@terascope/utils';
import type { S3Client } from './client-helpers/index.js';

export interface S3ClientConfig extends BaseConfig {
    sslEnabled?: boolean,
    certLocation?: string,
    caCertificate?: string,
    globalCaCertificate?: string,
    secretAccessKey?: string,
    accessKeyId?: string,
    maxRetries?: number
}

export interface S3ClientCredentials {
    accessKeyId: string;
    secretAccessKey: string;
}

export async function createS3Client(
    config: S3ClientConfig,
    logger = debugLogger('s3-client')
): Promise<S3Client> {
    logger.info(`Using S3 endpoint: ${config.endpoint}`);

    const finalConfig = await genFinalS3ClientConfig(config);

    // The aws v3 client logs every request and its metadata, it is too intrusive
    // so should only be used in trace mode otherwise it will log the body request
    // which has heavy performance implications
    if (logger.level() === 10) {
        finalConfig.logger = logger;
    }
    logger.debug(`createS3Client finalConfig:\n${JSON.stringify(finalConfig, null, 2)}`);

    return new BaseClient(finalConfig);
}

/**
 * Given an S3ClientConfig object, modify to make compatible with S3 Client V3
 * @param {S3ClientConfig} config Starting S3 client configuration object
 * @returns {S3ClientConfig} Final S3 client configuration object
 */
export async function genFinalS3ClientConfig(config: S3ClientConfig): Promise<BaseConfig> {
    if (config.maxRetries) {
        config.maxAttempts = config.maxRetries;
    }

    if (config.sslEnabled) {
        if (typeof config.endpoint === 'string' && !config.endpoint.includes('https')) {
            throw new Error(`S3 endpoint ${config.endpoint} must be https if sslEnabled`);
        }
        const httpOptions = await createHttpOptions(config);
        const requestHandlerOptions = createRequestHandlerOptions(httpOptions);
        config.requestHandler = new NodeHttpHandler(requestHandlerOptions);
    }

    if (!config.sslEnabled && typeof config.endpoint === 'string' && config.endpoint?.includes('https')) {
        throw new Error(`S3 endpoint ${config.endpoint} cannot be https if sslEnabled is false`);
    }

    if (!config.credentials) {
        config.credentials = createCredentialsObject(config);
    }

    return config;
}

/**
 * Given the S3 client configuration, return httpOptions containig CA certs.
 * @param {S3ClientConfig} config S3 client configuration object
 * @returns {Promise<HttpsAgentOptions>} Options used to make HttpsAgent
 */
export async function createHttpOptions(
    config: S3ClientConfig
): Promise<HttpsAgentOptions> {
    // pull certLocation from env
    // https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/node-registering-certs.html
    // Instead of updating the client, we can just update the config before creating the client

    const terafoundationCerts: string[] = [];
    const defaultNodeCerts = tls.rootCertificates;

    // Deprecated
    if (config.certLocation) {
        const certPathFound = await fs.existsSync(config.certLocation);
        if (certPathFound) {
            terafoundationCerts.push(await fs.readFileSync(config.certLocation, 'ascii'));
        } else {
            throw new Error(`No cert path was found in config.certLocation: "${config.certLocation}"`);
        }
    }

    if (config.caCertificate) {
        terafoundationCerts.push(config.caCertificate);
    }

    if (config.globalCaCertificate) {
        terafoundationCerts.push(config.globalCaCertificate);
    }

    const allCerts: string[] = terafoundationCerts.concat(defaultNodeCerts);

    return {
        rejectUnauthorized: true,
        ca: allCerts
    };
}

/**
 * Given HttpAgentOptions, create NodeHttpHandlerOptions
 * @param {HttpsAgentOptions} httpOptions
 * @returns {NodeHttpHandlerOptions}
 */
export function createRequestHandlerOptions(
    httpOptions: HttpsAgentOptions
): NodeHttpHandlerOptions {
    return { httpsAgent: new Agent(httpOptions) };
}

/**
 * Given an S3 client configuration, create a credentials object
 * containing accessKeyId and secretAccessKey.
 * S3Client V3 expects accessKeyId and secretAccessKey to be within a credentials object.
 * @param {S3ClientConfig} config S3 client configuration object
 * @returns {S3ClientCredentials}
 */
export function createCredentialsObject(
    config: S3ClientConfig
): S3ClientCredentials {
    if (!config.accessKeyId) {
        throw new Error('S3 accessKeyId must be defined in S3ClientConfig');
    }
    if (!config.secretAccessKey) {
        throw new Error('S3 secretAccessKey must be defined in S3ClientConfig');
    }
    const { accessKeyId, secretAccessKey } = config;
    return {
        accessKeyId,
        secretAccessKey
    };
}
