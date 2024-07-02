import 'jest-extended';
import { Compression, Compressor } from '../../src/index.js';

describe('Compressor', () => {
    it('can work with gzip', async () => {
        const compression = Compression.gzip;
        const formatter = new Compressor(compression);
        const data = 'I am a string';

        const compressResults = await formatter.compress(data);

        expect(Buffer.isBuffer(compressResults)).toBeTrue();

        const results = await formatter.decompress(compressResults);

        expect(results).toEqual(data);
    });

    it('can work with none', async () => {
        const compression = Compression.none;
        const formatter = new Compressor(compression);
        const data = 'I am a string';

        const compressResults = await formatter.compress(data);

        expect(compressResults.toString()).toEqual(data);

        const results = await formatter.decompress(compressResults);

        expect(results.toString()).toEqual(data);
    });

    it('can work with lz4', async () => {
        const compression = Compression.lz4;
        const formatter = new Compressor(compression);
        const data = 'I am a string';

        const compressResults = await formatter.compress(data);

        expect(Buffer.isBuffer(compressResults)).toBeTrue();

        const results = await formatter.decompress(compressResults);

        expect(results).toEqual(data);
    });
});
