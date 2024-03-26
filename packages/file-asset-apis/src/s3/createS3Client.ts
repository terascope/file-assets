import fs from 'fs-extra';
import { Agent, AgentOptions as HttpsAgentOptions } from 'https';
import { S3Client as BaseClient } from '@aws-sdk/client-s3';
import { NodeHttpHandler, NodeHttpHandlerOptions } from '@smithy/node-http-handler';
import type { S3ClientConfig as baseConfig } from '@aws-sdk/client-s3';
import {
    debugLogger, has, isEmpty, isNumber
} from '@terascope/utils';
import type { S3Client } from './client-types';

export interface S3ClientConfig extends baseConfig {
    sslEnabled?: boolean,
    certLocation?: string,
    httpOptions?: HttpsAgentOptions,
    handlerOptions?: Pick<NodeHttpHandlerOptions, 'requestTimeout'|'connectionTimeout'>,
    secretAccessKey?: string,
    accessKeyId?: string
}

export async function createS3Client(
    config: S3ClientConfig,
    logger = debugLogger('s3-client')
): Promise<S3Client> {
    // logger.debug(`====> config:\n${JSON.stringify(config, null, 2)}`);
    // logger.info(`Using S3 endpoint: ${config.endpoint}`);

    // The aws v3 client logs every request and its metadata, it is too intrusive
    // so should only be used in trace mode otherwise it will log the body request
    // which has heavy performance implications
    if (logger.level() === 10) {
        config.logger = logger;
    }

    const finalConfig = await genS3ClientConfig(config);
    logger.debug(`====> finalConfig:\n${JSON.stringify(finalConfig, null, 2)}`);
    return new BaseClient(finalConfig);
}

/**
 * given the terafoundation S3 connector configuration, generate the AWS S3
 * client configuration object
 * @param {S3ClientConfig} config terafoundation S3 connector configuration object
 * @returns AWS S3 client configuration object
 */
export async function genS3ClientConfig(config: S3ClientConfig) {
    let httpOptions = Object.assign(
        config.httpOptions || {}
    );

    // pull certLocation from env
    // https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/node-registering-certs.html
    // Instead of updating the client, we can just update the config before creating the client
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

        httpOptions = {
            ...httpOptions,
            rejectUnauthorized: true,
            ca: [certs]
        };
    }

    const { connectionTimeout, requestTimeout } = config.handlerOptions || {};
    const requestHandlerOptions = {
        ...isNumber(connectionTimeout) && { connectionTimeout },
        ...isNumber(requestTimeout) && { requestTimeout },
        ...!isEmpty(httpOptions) && new Agent(httpOptions)
    };

    // console.log(`requestHandlerOptions: ${JSON.stringify(requestHandlerOptions, null, 2)}`);
    if (!isEmpty(requestHandlerOptions)) {
        config.requestHandler = new NodeHttpHandler(requestHandlerOptions);
    }

    // config specified old style, need to move top level values into credentials
    if (!has(config, 'credentials') && has(config, 'accessKeyId') && has(config, 'secretAccessKey')) {
        const { accessKeyId = '', secretAccessKey = '' } = config;
        config.credentials = {
            accessKeyId,
            secretAccessKey
        };
    }

    // specified in terafoundation connector config but is now maxAttempts
    if (!has(config, 'maxAttempts') && has(config, 'maxRetries')) {
        config.maxAttempts = (config as any).maxRetries;
    }
    return config;
}
