import { parsePath } from '../../packages/file-asset-apis';

describe('s3 reader helpers', () => {
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
