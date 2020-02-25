import { DataEntity } from '@terascope/job-components';
import { getName, parsePath } from '../../asset/src/__lib/fileName';
import { parseForFile } from '../../asset/src/__lib/parser';

describe('File asset supporting library', () => {
    describe('parser module', () => {
        it('errors with invalid formats.', async () => {
            await expect(parseForFile([new DataEntity({})], { format: 'invalid' }, {})).rejects.toThrowError('Unsupported output format "invalid"');
            /* let error;
            try {
                await parseForFile([{}], { format: 'invalid' });
            } catch (e) {
                error = e;
            }
            expect(error).toEqual(new Error('Unsupported output format "invalid"')); */
        });
        it('returns null for empty records.', async () => {
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
        it('properly identifies buckets and object prefixes in a filepath', () => {
            /* Eight possible inputs
             * - with(out) leading '/'
             * - with(out) trailing '/'
             * - with(out) subdir/prefix
             */
            const path1 = 'data';
            const path2 = '/data';
            const path3 = 'data/';
            const path4 = '/data/';
            const path5 = 'data/export';
            const path6 = '/data/export';
            const path7 = 'data/export/';
            const path8 = '/data/export/';

            const info1 = parsePath(path1);
            const info2 = parsePath(path2);
            const info3 = parsePath(path3);
            const info4 = parsePath(path4);
            const info5 = parsePath(path5);
            const info6 = parsePath(path6);
            const info7 = parsePath(path7);
            const info8 = parsePath(path8);

            expect(info1).toEqual({
                bucket: 'data',
                prefix: ''
            });
            expect(info2).toEqual({
                bucket: 'data',
                prefix: ''
            });
            expect(info3).toEqual({
                bucket: 'data',
                prefix: ''
            });
            expect(info4).toEqual({
                bucket: 'data',
                prefix: ''
            });
            expect(info5).toEqual({
                bucket: 'data',
                prefix: 'export/'
            });
            expect(info6).toEqual({
                bucket: 'data',
                prefix: 'export/'
            });
            expect(info7).toEqual({
                bucket: 'data',
                prefix: 'export/'
            });
            expect(info8).toEqual({
                bucket: 'data',
                prefix: 'export/'
            });
        });
    });
});
