import 'jest-extended';
import { debugLogger } from '@terascope/utils';
import { S3Reader } from '../../src';
import { makeClient } from './helpers';

describe('S3Reader Asset APIs', () => {
    const client = makeClient();
    const logger = debugLogger('s3-reader-asset-api');

    it('will throw if file_per_slice is false or undefined', () => {
        const errMsg = 'Invalid parameter "file_per_slice", it must be set to true, cannot be append data to S3 objects';
        // @ts-expect-error TODO: remove this
        expect(() => new S3Reader(client, { file_per_slice: false }, logger)).toThrowError(errMsg);
        expect(
            // @ts-expect-error TODO: remove this
            () => new S3Reader(client, { file_per_slice: undefined }, logger)
        ).toThrowError(errMsg);
    });
});
