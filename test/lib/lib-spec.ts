import { DataEntity } from '@terascope/job-components';
import { getName } from '../../asset/src/__lib/fileName';
import { parseForFile } from '../../asset/src/__lib/parser';

describe('File asset supporting library', () => {
    describe('parser module', () => {
        it('errors with invalid formats.', async () => {
            // @ts-expect-error
            await expect(parseForFile([new DataEntity({})], { format: 'invalid' }, {})).rejects.toThrowError('Unsupported output format "invalid"');
        });

        it('returns null for empty records.', async () => {
            // @ts-expect-error
            expect(await parseForFile(null, { format: 'tsv' }, {})).toEqual(null);
        });
    });

    describe('fileName module', () => {
        it('adds a file extension.', () => {
            expect(getName(
                'worker1',
                2,
                { filePerSlice: true, extension: '.txt', filePath: '/data/' }
            )).toEqual('/data/worker1.2.txt');
        });
    });
});
