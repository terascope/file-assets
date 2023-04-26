import fs from 'fs-extra';
import { Agent } from 'https';
import { S3Client as BaseClient } from '@aws-sdk/client-s3';
import { NodeHttpHandler } from '@aws-sdk/node-http-handler';
import type { S3ClientConfig as baseConfig } from '@aws-sdk/client-s3';
import { debugLogger, has } from '@terascope/utils';
import type { S3Client } from './client-types';

export interface S3ClientConfig extends baseConfig {
    sslEnabled?: boolean,
    certLocation?: string,
    httpOptions?: object,
    secretAccessKey?: string,
    accessKeyId?: string
}

export async function createS3Client(
    config: S3ClientConfig,
    logger = debugLogger('s3-client')
): Promise<S3Client> {
    logger.info(`Using S3 endpoint: ${config.endpoint}`);
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

        const configHttpOptions = config.httpOptions ?? {};

        const httpOptions = Object.assign(
            { rejectUnauthorized: true },
            configHttpOptions,
            { ca: [certs] }
        );

        config.requestHandler = new NodeHttpHandler({
            httpsAgent: new Agent(httpOptions)
        });
    }

    // config specified old style, need to move top level values into credentials
    if (!has(config, 'credentials') && has(config, 'accessKeyId') && has(config, 'secretAccessKey')) {
        const { accessKeyId, secretAccessKey } = config;
        config.credentials = {
            accessKeyId,
            secretAccessKey
        } as any;
    }

    return new BaseClient(config);
}
