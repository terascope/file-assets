import 'jest-extended';
import {
    ChunkGenerator, Formatter, Compressor, Format, Compression, Chunk
} from '../../src';

describe('ChunkGenerator', () => {
    describe(`when the format is ${Format.json}`, () => {
        describe('when constructed with a empty slice', () => {
            let gen: ChunkGenerator;
            beforeAll(() => {
                gen = new ChunkGenerator(
                    new Formatter({
                        id: 'foo',
                        path: 'foo',
                        format: Format.json,
                    }),
                    new Compressor(Compression.none),
                    []
                );
            });

            it('should return no chunks', async () => {
                await expect(toArray(gen)).resolves.toEqual([]);
            });
        });

        describe('when constructed with a small slice', () => {
            let gen: ChunkGenerator;
            const input = [{ foo: 'bar', count: 1 }, { foo: 'baz', count: 1 }];
            beforeAll(() => {
                gen = new ChunkGenerator(
                    new Formatter({
                        id: 'foo',
                        path: 'foo',
                        format: Format.json,
                    }),
                    new Compressor(Compression.none),
                    input
                );
            });

            it('should return a small chunk', async () => {
                const expected: Chunk[] = [{
                    index: 1,
                    data: Buffer.from(`${JSON.stringify(input)}\n`),
                    has_more: false,
                }];
                await expect(toArray(gen)).resolves.toEqual(expected);
            });
        });

        // describe('when constructed with a large slice', () => {
        //     let gen: ChunkGenerator;

        //     const input = new Array(1000).map((_val, index) => ({
        //         foo: 'bar',
        //         count: index,
        //         boo: true
        //     }));

        //     beforeAll(() => {
        //         gen = new ChunkGenerator(
        //             new Formatter({
        //                 id: 'foo',
        //                 path: 'foo',
        //                 format: Format.ldjson,
        //             }),
        //             new Compressor(Compression.none),
        //             input
        //         );
        //     });

        //     it('should return a small chunk', async () => {
        //         const expected: Chunk[] = [{
        //             index: 1,
        //             data: Buffer.from(`${JSON.stringify(input)}\n`),
        //             has_more: false,
        //         }];
        //         await expect(toArray(gen)).resolves.toEqual(expected);
        //     });
        // });
    });
});

async function toArray(gen: ChunkGenerator): Promise<Chunk[]> {
    const result: Chunk[ ] = [];
    for await (const chunk of gen) {
        result.push(chunk);
    }
    return result;
}
