import fs from 'fs-extra';
import { Agent, AgentOptions as HttpsAgentOptions } from 'https';
import { S3Client as BaseClient } from '@aws-sdk/client-s3';
import { NodeHttpHandler, NodeHttpHandlerOptions } from '@smithy/node-http-handler';
import type { S3ClientConfig as baseS3Config } from '@aws-sdk/client-s3';
import {
    cloneDeep,
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
    let intermediateConfig = await addCertsIfSSLEnabled(startConfig);
    intermediateConfig = addRequestHandler(intermediateConfig);
    intermediateConfig = moveCredentialsIntoObject(intermediateConfig);
    intermediateConfig = mapMaxRetriesToMaxAttempts(intermediateConfig);
    const finalConfig = removeExtendedConfigKeys(intermediateConfig);
    return finalConfig;
}

/**
 * Given the terafoundation S3 connector configuration, if sslEnabled is true,
 * add CA certs and 'rejectUnauthorized: true' to httpOptions.
 * @param {S3ClientConfig} startConfig terafoundation S3 connector configuration object
 * @returns {Promise<S3ClientConfig>} AWS S3 client EXTENDED configuration object
 */
export async function addCertsIfSSLEnabled(startConfig: S3ClientConfig): Promise<S3ClientConfig> {
    // pull certLocation from env
    // https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/node-registering-certs.html
    // Instead of updating the client, we can just update the config before creating the client

    let newOptions: HttpsAgentOptions = Object.assign(
        startConfig.httpOptions || {}
    );

    if (startConfig.sslEnabled) {
        const certPath = startConfig.certLocation ?? '/etc/ssl/certs/ca-certificates.crt';
        const pathFound = await fs.exists(certPath);

        if (!pathFound) {
            throw new Error(
                `No cert path was found in config.certLocation: "${startConfig.certLocation}" or in default "/etc/ssl/certs/ca-certificates.crt" location`
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

    const endConfig = cloneDeep(startConfig);
    endConfig.httpOptions = newOptions;
    return endConfig;
}

/**
 * Given the terafoundation S3 connector configuration, create a
 * NodeHttpHandler and add it to the config
 * @param {S3ClientConfig} startConfig terafoundation S3 connector configuration object
 * @returns {Promise<S3ClientConfig>} AWS S3 client EXTENDED configuration object
 */
export function addRequestHandler(startConfig: S3ClientConfig): S3ClientConfig {
    const { connectionTimeout, requestTimeout } = startConfig.handlerOptions || {};
    const requestHandlerOptions = {
        ...isNumber(connectionTimeout) && { connectionTimeout },
        ...isNumber(requestTimeout) && { requestTimeout },
        ...!isEmpty(startConfig.httpOptions) && { httpsAgent: new Agent(startConfig.httpOptions) }
    };
    if (!isEmpty(requestHandlerOptions)) {
        const endConfig = cloneDeep(startConfig);
        endConfig.requestHandler = new NodeHttpHandler(requestHandlerOptions);
        return endConfig;
    }
    return startConfig;
}

/**
 * Given the terafoundation S3 connector configuration, if accessKeyId and
 * secretAccessKey are unique top level keys, move them into a credentials object.
 * The S3 connector config is specified in an old style that is no longer supported.
 * @param {S3ClientConfig} startConfig terafoundation S3 connector configuration object
 * @returns {Promise<S3ClientConfig>} AWS S3 client EXTENDED configuration object
 */
export function moveCredentialsIntoObject(startConfig: S3ClientConfig): S3ClientConfig {
    const endConfig = cloneDeep(startConfig);
    if (!has(startConfig, 'credentials') && has(startConfig, 'accessKeyId') && has(startConfig, 'secretAccessKey')) {
        const { accessKeyId = '', secretAccessKey = '' } = startConfig;
        endConfig.credentials = {
            accessKeyId,
            secretAccessKey
        };
        delete endConfig.accessKeyId;
        delete endConfig.secretAccessKey;
    }
    return endConfig;
}

/**
 * Given the terafoundation S3 connector configuration, if maxRetries is specified
 * copy its value into the maxAttempts key and remove matRetries
 * MaxRetries is the key name in terafoundation connector config but the S3 client
 * renamed the key to maxAttempts.
 * @param {S3ClientConfig} startConfig terafoundation S3 connector configuration object
 * @returns {Promise<S3ClientConfig>} AWS S3 client EXTENDED configuration object
 */
export function mapMaxRetriesToMaxAttempts(startConfig: S3ClientConfig): S3ClientConfig {
    const endConfig = cloneDeep(startConfig);
    if (!has(startConfig, 'maxAttempts') && has(startConfig, 'maxRetries')) {
        endConfig.maxAttempts = (startConfig as any).maxRetries;
    }
    delete (endConfig as any).maxRetries;
    return endConfig;
}

/**
 * Given the terafoundation S3 connector configuration, remove all extended keys
 * that belong solely to the S3ClientConfig class
 * @param {S3ClientConfig} startConfig terafoundation S3 connector configuration object
 * @returns {Promise<S3ClientConfig>} AWS S3 client BASE configuration object
 */
export function removeExtendedConfigKeys(startConfig: S3ClientConfig): baseS3Config {
    const endConfig = cloneDeep(startConfig);
    delete endConfig.sslEnabled;
    delete endConfig.certLocation;
    delete endConfig.httpOptions;
    delete endConfig.handlerOptions;
    return endConfig;
}
