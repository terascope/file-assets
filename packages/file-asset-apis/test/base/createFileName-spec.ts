import {
    Compression, createFileName, Format, NameOptions
} from '../../src/index.js';

describe('createFileName', () => {
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

    it('can overwrite the extension for the file', () => {
        const config: NameOptions = {
            format: Format.csv,
            compression: Compression.none,
            filePerSlice: false,
            id,
            extension: 'foo'
        };

        const fileName = createFileName(testFile, config);

        expect(fileName).toEqual(`${baseFileName}.foo`);
    });

    it('can overwrite set the extension to nothing', () => {
        const config: NameOptions = {
            format: Format.csv,
            compression: Compression.lz4,
            filePerSlice: false,
            id,
            extension: ''
        };

        const fileName = createFileName(testFile, config);

        expect(fileName).toEqual(`${baseFileName}`);
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

        expect(
            () => {
                createFileName(1234 as unknown as string, config);
            }
        ).toThrowError('Invalid parameter filePath, it must be a string value');
        expect(
            () => createFileName(
                'test',
                {
                    ...config,
                    format: 'something else' as unknown as Format
                }
            )
        ).toThrowError('Invalid parameter format, it must be of type Format');
        expect(
            () => createFileName(
                'test',
                {
                    ...config,
                    compression: 'something else' as unknown as Compression
                }
            )
        ).toThrowError('Invalid parameter format, it must be of type Compression');
        expect(
            () => createFileName(
                'test',
                {
                    ...config,
                    id: null as unknown as string
                }
            )
        ).toThrowError('Invalid parameter id, it must be a string value');
        expect(
            () => createFileName(
                'test',
                {
                    ...config,
                    filePerSlice: 1234 as unknown as boolean
                }
            )
        ).toThrowError('Invalid parameter filePerSlice, it must be a boolean value');
        expect(
            () => createFileName(
                'test',
                {
                    ...config,
                    filePerSlice: true,
                    sliceCount: null as unknown as number
                }
            )
        ).toThrowError('Invalid parameter sliceCount, it must be provided when filePerSlice is set to true, and must be a number');
        expect(
            () => createFileName(
                'test',
                {
                    ...config,
                    extension: 6782 as unknown as string
                }
            )
        ).toThrowError('Invalid parameter extension, it must be a string value');
    });
});
