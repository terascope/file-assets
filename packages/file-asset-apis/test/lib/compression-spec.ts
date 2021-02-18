import 'jest-extended';
import { Compression, CompressionFormatter } from '../../src';

describe('CompressionFormatter', () => {
    it('can work with gzip', async () => {
        const compression = Compression.gzip;
        const formatter = new CompressionFormatter(compression);
        const data = 'I am a string';

        const compressResults = await formatter.compress(data);

        expect(Buffer.isBuffer(compressResults)).toBeTrue();

        const results = await formatter.decompress(compressResults);

        expect(results).toEqual(data);
    });

    it('can work with none', async () => {
        const compression = Compression.none;
        const formatter = new CompressionFormatter(compression);
        const data = 'I am a string';

        const compressResults = await formatter.compress(data);

        expect(compressResults).toEqual(data);

        const results = await formatter.decompress(compressResults);

        expect(results).toEqual(data);
    });

    it('can work with lz4', async () => {
        const compression = Compression.lz4;
        const formatter = new CompressionFormatter(compression);
        const data = 'I am a string';

        const compressResults = await formatter.compress(data);

        expect(Buffer.isBuffer(compressResults)).toBeTrue();

        const results = await formatter.decompress(compressResults);

        expect(results).toEqual(data);
    });
});
