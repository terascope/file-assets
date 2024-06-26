import 'jest-extended';
import fs from 'node:fs';
import { debugLogger, toNumber } from '@terascope/utils';
//@ts-expect-error
import fixtures from 'jest-fixtures';
import {
    FileSender, Format, Compression, LDJSONSenderConfig
} from '../../src/index.js';


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
        const config: LDJSONSenderConfig = {
            id: workerId,
            dynamic_routing: false,
            path: testDataDir,
            fields: [],
            concurrency: 4,
            format: Format.ldjson,
            extension: '',
            compression: Compression.none,
            line_delimiter: '\n',
            file_per_slice: true
        };

        expect(fs.readdirSync(testDataDir).length).toEqual(0);

        const fileSender = new FileSender(config, logger);

        await fileSender.send(data);

        await fileSender.send(data);

        await fileSender.send(data);

        const dirs = fs.readdirSync(testDataDir);
        const fileNumbers = dirs.map((name) => toNumber(name.split('.')[1]));

        expect(fileNumbers).toEqual([0, 1, 2]);
    });
});
