import { S3Client } from '@aws-sdk/client-s3';

/**
 * Args for `s3RequestWithRetry`. The params type comes from `func`,
 * so `params` must match what `func` expects.
 */
export type RetryArgs<Params, Response> = {
    client: S3Client;
    func: (client: S3Client, params: Params) => Promise<Response>;
    params: NoInfer<Params>;
};
