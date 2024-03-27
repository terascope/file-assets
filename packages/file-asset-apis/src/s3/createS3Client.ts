import fs from 'fs-extra';
import { Agent, AgentOptions as HttpsAgentOptions } from 'https';
import { S3Client as BaseClient } from '@aws-sdk/client-s3';
import { NodeHttpHandler, NodeHttpHandlerOptions } from '@smithy/node-http-handler';
import type { S3ClientConfig as baseS3Config } from '@aws-sdk/client-s3';
import {
    debugLogger, has, isEmpty, isNumber
} from '@terascope/utils';
import type { S3Client } from './client-types';

export interface S3ClientConfig extends baseS3Config {
    sslEnabled?: boolean,
    certLocation?: string,
    httpOptions?: HttpsAgentOptions,
    handlerOptions?: Pick<NodeHttpHandlerOptions, 'requestTimeout' | 'connectionTimeout'>,
    secretAccessKey?: string,
    accessKeyId?: string
}

export async function createS3Client(
    config: S3ClientConfig,
    logger = debugLogger('s3-client')
): Promise<S3Client> {
    logger.info(`Using S3 endpoint: ${config.endpoint}`);

    // The aws v3 client logs every request and its metadata, it is too intrusive
    // so should only be used in trace mode otherwise it will log the body request
    // which has heavy performance implications
    if (logger.level() === 10) {
        config.logger = logger;
    }

    const finalConfig: baseS3Config = await genS3ClientConfig(config);
    logger.debug(`createS3Client finalConfig:\n${JSON.stringify(finalConfig, null, 2)}`);
    return new BaseClient(finalConfig);
}

/**
 * Given the terafoundation S3 connector configuration, generate the AWS S3
 * client configuration object
 * @param {S3ClientConfig} startConfig terafoundation S3 connector configuration object
 * @returns {Promise<baseS3Config>} AWS S3 client BASE configuration object
 */
export async function genS3ClientConfig(startConfig: S3ClientConfig): Promise<baseS3Config> {
    let config = await addCertsIfSSLEnabled(startConfig);
    const requestHandlerOptions = createRequestHandlerOptions(config);
    config = addRequestHandler(config, requestHandlerOptions);
    config = moveCredentialsIntoObject(config);
    config = mapMaxRetriesToMaxAttempts(config);
    return removeExtendedConfigKeys(config);
}

/**
 * Given the terafoundation S3 connector configuration, if sslEnabled is true,
 * add CA certs and 'rejectUnauthorized: true' to httpOptions.
 * @param {S3ClientConfig} config terafoundation S3 connector configuration object
 * @returns {Promise<S3ClientConfig>} AWS S3 client EXTENDED configuration object
 */
export async function addCertsIfSSLEnabled(config: S3ClientConfig): Promise<S3ClientConfig> {
    // pull certLocation from env
    // https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/node-registering-certs.html
    // Instead of updating the client, we can just update the config before creating the client

    let newOptions: HttpsAgentOptions = Object.assign(
        config.httpOptions || {}
    );

    if (config.sslEnabled) {
        const certPath = config.certLocation ?? '/etc/ssl/certs/ca-certificates.crt';
        const pathFound = await fs.exists(certPath);

        if (!pathFound) {
            throw new Error(
                `No cert path was found in config.certLocation: "${config.certLocation}" or in default "/etc/ssl/certs/ca-certificates.crt" location`
            );
        }
        // Assumes all certs needed are in a single bundle
        const certs = await fs.readFile(certPath);

        newOptions = {
            ...newOptions,
            rejectUnauthorized: true,
            ca: [certs]
        };
    }

    config.httpOptions = newOptions;
    return config;
}

/**
 * Given the terafoundation S3 connector configuration, create NodeHttpHandlerOptions
 * @param {S3ClientConfig} startConfig terafoundation S3 connector configuration object
 * @returns {NodeHttpHandlerOptions} NodeHttpHandlerOptions
 */
export function createRequestHandlerOptions(startConfig: S3ClientConfig): NodeHttpHandlerOptions {
    const { connectionTimeout, requestTimeout } = startConfig.handlerOptions || {};
    return {
        ...isNumber(connectionTimeout) && { connectionTimeout },
        ...isNumber(requestTimeout) && { requestTimeout },
        ...!isEmpty(startConfig.httpOptions) && { httpsAgent: new Agent(startConfig.httpOptions) }
    };
}

/**
 * Given the terafoundation S3 connector configuration and NodeHttpHandlerOptions,
 * create a NodeHttpHandler and add it to the config
 * @param {S3ClientConfig} config terafoundation S3 connector configuration object
 * @returns {S3ClientConfig} AWS S3 client EXTENDED configuration object
 */
export function addRequestHandler(
    config: S3ClientConfig,
    requestHandlerOptions: NodeHttpHandlerOptions
): S3ClientConfig {
    if (!isEmpty(requestHandlerOptions)) {
        config.requestHandler = new NodeHttpHandler(requestHandlerOptions);
        return config;
    }
    return config;
}

/**
 * Given the terafoundation S3 connector configuration, if accessKeyId and
 * secretAccessKey are unique top level keys, move them into a credentials object.
 * The S3 connector config is specified in an old style that is no longer supported.
 * @param {S3ClientConfig} config terafoundation S3 connector configuration object
 * @returns {S3ClientConfig} AWS S3 client EXTENDED configuration object
 */
export function moveCredentialsIntoObject(config: S3ClientConfig): S3ClientConfig {
    if (!has(config, 'credentials') && has(config, 'accessKeyId') && has(config, 'secretAccessKey')) {
        const { accessKeyId = '', secretAccessKey = '' } = config;
        config.credentials = {
            accessKeyId,
            secretAccessKey
        };
        delete config.accessKeyId;
        delete config.secretAccessKey;
    }
    return config;
}

/**
 * Given the terafoundation S3 connector configuration, if maxRetries is specified
 * copy its value into the maxAttempts key and remove matRetries
 * MaxRetries is the key name in terafoundation connector config but the S3 client
 * renamed the key to maxAttempts.
 * @param {S3ClientConfig} config terafoundation S3 connector configuration object
 * @returns {S3ClientConfig} AWS S3 client EXTENDED configuration object
 */
export function mapMaxRetriesToMaxAttempts(config: S3ClientConfig): S3ClientConfig {
    if (!has(config, 'maxAttempts') && has(config, 'maxRetries')) {
        config.maxAttempts = (config as any).maxRetries;
    }
    delete (config as any).maxRetries;
    return config;
}

/**
 * Given the terafoundation S3 connector configuration, remove all extended keys
 * that belong solely to the S3ClientConfig class
 * @param {S3ClientConfig} config terafoundation S3 connector configuration object
 * @returns {S3ClientConfig} AWS S3 client BASE configuration object
 */
export function removeExtendedConfigKeys(config: S3ClientConfig): baseS3Config {
    delete config.sslEnabled;
    delete config.certLocation;
    delete config.httpOptions;
    delete config.handlerOptions;
    return config;
}
