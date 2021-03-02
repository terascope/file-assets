import 'jest-extended';
import {
    ChunkGenerator, Formatter, Compressor, Format, Compression
} from '../../src';

describe('ChunkGenerator', () => {
    describe('when constructed a empty slice', () => {
        let gen: ChunkGenerator;
        beforeAll(() => {
            gen = new ChunkGenerator(
                new Formatter({
                    format: Format.json,
                } as any), // FIXME
                new Compressor(Compression.none),
                []
            );
        });

        it('should have return no chunks', () => {
            expect([...gen]).toEqual([]);
        });
    });
});
