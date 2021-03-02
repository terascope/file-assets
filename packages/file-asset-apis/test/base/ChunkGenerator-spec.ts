import 'jest-extended';
import {
    ChunkGenerator, FileFormatter, CompressionFormatter, Format, Compression
} from '../../src';

describe('ChunkGenerator', () => {
    describe('when constructed a empty slice', () => {
        let gen: ChunkGenerator;
        beforeAll(() => {
            gen = new ChunkGenerator(
                new FileFormatter({
                    format: Format.json,
                } as any), // FIXME
                new CompressionFormatter(Compression.none),
                []
            );
        });

        it('should have return no chunks', () => {
            expect([...gen]).toEqual([]);
        });
    });
});
