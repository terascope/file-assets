'use strict';

const { getName, parsePath } = require('../../asset/lib/fileName');
const { parseForFile } = require('../../asset/lib/parser');

describe('File asset supporting library', () => {
    describe('parser module', () => {
        it('errors with invalid formats.', async () => {
            let error;
            try {
                await parseForFile(null, { format: 'invalid' });
            } catch (e) {
                error = e;
            }
            expect(error).toEqual(new Error('Unsupported output format "invalid"'));
        });
        it('returns null for empty records.', async () => {
            expect(await parseForFile(null, { format: 'tsv' })).toEqual(null);
        });
    });
    describe('fileName module', () => {
        it('adds a file extension.', () => {
            expect(getName(
                'worker1',
                2,
                { file_per_slice: true, extension: 'txt', path: '/data/' }
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
            const path5 = 'data/export_';
            const path6 = '/data/export_';
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
                prefix: 'export_'
            });
            expect(info6).toEqual({
                bucket: 'data',
                prefix: 'export_'
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
