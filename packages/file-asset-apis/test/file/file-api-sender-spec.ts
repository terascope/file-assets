import 'jest-extended';
import fs from 'fs';
import { debugLogger, toNumber } from '@terascope/utils';
import {
    FileSender, ChunkedSenderConfig, Format, Compression
} from '../../src';

const fixtures = require('jest-fixtures');

describe('File Asset Sender API', () => {
    const logger = debugLogger('file-asset-API-sender');
    const workerId = 'some-id';

    const data = [
        { some: 'data' },
        { other: 'stuff' },
        { last: 'record' }
    ];

    it('can send data and respect slice', async () => {
        const testDataDir = await fixtures.createTempDir();
        const config: ChunkedSenderConfig = {
            worker_id: workerId,
            dynamic_routing: false,
            size: 2000,
            connection: 'default',
            remove_header: false,
            ignore_empty: true,
            extra_args: {},
            path: testDataDir,
            fields: [],
            concurrency: 4,
            format: Format.ldjson,
            extension: '',
            compression: Compression.none,
            field_delimiter: '',
            line_delimiter: '\n',
            include_header: false,
            file_per_slice: true
        };

        expect(fs.readdirSync(testDataDir).length).toEqual(0);

        const fileSender = new FileSender(config, logger);

        await fileSender.send(data);

        await fileSender.send(data);

        await fileSender.send(data);

        const dirs = fs.readdirSync(testDataDir);

        const fileNumbers = dirs.map((name) => toNumber(name[name.length - 1]));

        expect(fileNumbers).toEqual([0, 1, 2]);
    });
});
