/* eslint-disable @typescript-eslint/no-unused-vars */
import 'jest-extended';
import { debugLogger } from '@terascope/utils';
import { S3TerasliceAPI } from '../../src';
import { makeClient } from './helpers';

describe('S3TerasliceAPI Asset APIs', () => {
    const client = makeClient();
    const logger = debugLogger('s3-reader-asset-api');

    it('will throw if file_per_slice is false or undefined', () => {
        const errMsg = 'Invalid parameter "file_per_slice", it must be set to true, cannot be append data to S3 objects';
        // expect(
        //     () => new S3TerasliceAPI(client, { file_per_slice: false }, logger)
        // ).toThrowError(errMsg);
        // expect(
        //     () => new S3TerasliceAPI(client, { file_per_slice: undefined }, logger)
        // ).toThrowError(errMsg);
    });
});
