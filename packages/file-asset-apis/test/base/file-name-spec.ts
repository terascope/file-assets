import {
    Compression, createFileName, Format, NameOptions
} from '../../src';

describe('can create formatted filename', () => {
    const testFile = 'testFile';
    const id = 'some-id';
    const baseFileName = `${testFile}/${id}`;

    it('can make a ldjson file', () => {
        const config: NameOptions = {
            format: Format.ldjson,
            compression: Compression.none,
            filePerSlice: false,
            id
        };

        const fileName = createFileName(testFile, config);

        expect(fileName).toEqual(`${baseFileName}.ldjson`);
    });

    it('can make a json file', () => {
        const config: NameOptions = {
            format: Format.json,
            compression: Compression.none,
            filePerSlice: true,
            id,
            sliceCount: 25
        };

        const fileName = createFileName(testFile, config);

        expect(fileName).toEqual(`${baseFileName}.25.json`);
    });

    it('can make a tsv file', () => {
        const config: NameOptions = {
            format: Format.tsv,
            compression: Compression.none,
            filePerSlice: false,
            id
        };

        const fileName = createFileName(testFile, config);

        expect(fileName).toEqual(`${baseFileName}.tsv`);
    });

    it('can make a csv file', () => {
        const config: NameOptions = {
            format: Format.csv,
            compression: Compression.none,
            filePerSlice: false,
            id
        };

        const fileName = createFileName(testFile, config);

        expect(fileName).toEqual(`${baseFileName}.csv`);
    });

    it('can make a raw file (no modifiers are given)', () => {
        const config: NameOptions = {
            format: Format.raw,
            compression: Compression.none,
            filePerSlice: false,
            id
        };

        const fileName = createFileName(testFile, config);

        expect(fileName).toEqual(`${baseFileName}`);
    });

    it('can validate parameters', () => {
        const config: NameOptions = {
            format: Format.ldjson,
            compression: Compression.none,
            filePerSlice: false,
            id
        };

        // @ts-expect-error
        expect(() => createFileName(1234, config)).toThrowError();

        // @ts-expect-error
        expect(() => createFileName('test', { ...config, format: 'something else' })).toThrowError();

        // @ts-expect-error
        expect(() => createFileName('test', { ...config, compression: 'something else' })).toThrowError();

        // @ts-expect-error
        expect(() => createFileName('test', { ...config, id: null })).toThrowError();

        // @ts-expect-error
        expect(() => createFileName('test', { ...config, filePerSlice: 1234 })).toThrowError();

        // @ts-expect-error
        expect(() => createFileName('test', { ...config, filePerSlice: true, sliceCount: null })).toThrowError();

        // @ts-expect-error
        expect(() => createFileName('test', { ...config, extension: 6782 })).toThrowError();
    });
});
