import { Overwrite, times } from '@terascope/utils';
import 'jest-extended';
import {
    ChunkGenerator, Formatter, Compressor,
    Format, Compression, Chunk
} from '../../src/index.js';

describe('ChunkGenerator', () => {
    const CHUNK_SIZE = 1024; // 1kib
    const limits = { maxBytes: CHUNK_SIZE, minBytes: CHUNK_SIZE };

    describe(`when the format is ${Format.json}`, () => {
        describe('when constructed with a empty slice', () => {
            let gen: ChunkGenerator;
            beforeAll(() => {
                gen = new ChunkGenerator(
                    new Formatter({
                        format: Format.json,
                    }),
                    new Compressor(Compression.none),
                    [],
                    undefined,
                    limits
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
                        format: Format.json,
                    }),
                    new Compressor(Compression.none),
                    input,
                    undefined,
                    limits
                );
            });

            it('should return a small chunk', async () => {
                const expected: TestChunk[] = [{
                    index: 0,
                    data: `${JSON.stringify(input)}\n`,
                    has_more: false
                }];
                await expect(toArray(gen)).resolves.toEqual(expected);
            });
        });

        describe('when constructed with a large slice', () => {
            let gen: ChunkGenerator;

            let input: Record<string, any>[];

            beforeAll(() => {
                // don't change this count without good reason
                // since it will break the tests
                input = times(200, (index) => ({
                    count: index,
                }));

                gen = new ChunkGenerator(
                    new Formatter({
                        format: Format.json,
                    }),
                    new Compressor(Compression.none),
                    input,
                    undefined,
                    limits
                );
            });

            it('should correctly determine that this cannot processed per row', () => {
                expect(gen.isRowOptimized()).toBeFalse();
            });

            it('should return a more than one chunk', async () => {
                const wholeBuffer = Buffer.from(`${JSON.stringify(input)}\n`);
                // we just need to ensure that our test will work
                expect(wholeBuffer.length).toBeGreaterThan(CHUNK_SIZE);

                const expected: TestChunk[] = [{
                    index: 0,
                    data: wholeBuffer
                        .subarray(0, CHUNK_SIZE)
                        .toString(),
                    has_more: true
                },
                {
                    index: 1,
                    data: wholeBuffer
                        .subarray(CHUNK_SIZE, CHUNK_SIZE * 2)
                        .toString(),
                    has_more: true
                },
                {
                    index: 2,
                    data: wholeBuffer
                        .subarray(CHUNK_SIZE * 2, CHUNK_SIZE * 3)
                        .toString(),
                    has_more: false
                }];
                await expect(toArray(gen)).resolves.toEqual(expected);
            });
        });
    });

    describe(`when the format is ${Format.csv}`, () => {
        describe('when constructed with a empty row', () => {
            let gen: ChunkGenerator;
            beforeAll(() => {
                gen = new ChunkGenerator(
                    new Formatter({
                        format: Format.csv,
                    }),
                    new Compressor(Compression.none),
                    [{}],
                    undefined,
                    limits
                );
            });

            it('should return no chunks', async () => {
                await expect(toArray(gen)).resolves.toEqual([]);
            });
        });
    });

    describe(`when the format is ${Format.ldjson} and ${Compression.lz4} compression`, () => {
        describe('when constructed with a empty slice', () => {
            let gen: ChunkGenerator;
            beforeAll(() => {
                gen = new ChunkGenerator(
                    new Formatter({
                        format: Format.ldjson,
                    }),
                    new Compressor(Compression.lz4),
                    [],
                    undefined,
                    limits
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
                        format: Format.ldjson,
                    }),
                    new Compressor(Compression.lz4),
                    input,
                    undefined,
                    limits
                );
            });

            it('should return a small chunk', async () => {
                const wholeBuffer = await gen.compressor.compress(
                    gen.formatter.format(input)
                );
                const expected: TestChunk[] = [{
                    index: 0,
                    data: wholeBuffer.toString(),
                    has_more: false
                }];
                await expect(toArray(gen)).resolves.toEqual(expected);
            });
        });

        describe('when constructed with a large slice', () => {
            let gen: ChunkGenerator;

            let input: Record<string, any>[];

            beforeAll(() => {
                // don't change this count without good reason
                // since it will break the tests
                input = times(500, (index) => ({
                    count: index,
                }));

                gen = new ChunkGenerator(
                    new Formatter({
                        format: Format.ldjson,
                    }),
                    new Compressor(Compression.lz4),
                    input,
                    undefined,
                    limits
                );
            });

            it('should correctly determine that this cannot processed per row', () => {
                expect(gen.isRowOptimized()).toBeFalse();
            });

            it('should return a more than one chunk', async () => {
                const wholeBuffer = await gen.compressor.compress(
                    gen.formatter.format(input)
                );
                // we just need to ensure that our test will work
                expect(wholeBuffer.length).toBeGreaterThan(CHUNK_SIZE);

                const expected: TestChunk[] = [{
                    index: 0,
                    data: wholeBuffer
                        .subarray(0, CHUNK_SIZE)
                        .toString(),
                    has_more: true
                },
                {
                    index: 1,
                    data: wholeBuffer
                        .subarray(CHUNK_SIZE, CHUNK_SIZE * 2)
                        .toString(),
                    has_more: false
                }];
                await expect(toArray(gen)).resolves.toEqual(expected);
            });
        });
    });

    describe(`when the format is ${Format.ldjson} and has no compression`, () => {
        describe('when constructed with a empty slice', () => {
            let gen: ChunkGenerator;
            beforeAll(() => {
                gen = new ChunkGenerator(
                    new Formatter({
                        format: Format.ldjson,
                    }),
                    new Compressor(Compression.none),
                    [],
                    undefined,
                    limits
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
                        format: Format.ldjson,
                    }),
                    new Compressor(Compression.none),
                    input,
                    undefined,
                    limits
                );
            });

            it('should return a small chunk', async () => {
                const wholeBuffer = await gen.compressor.compress(
                    gen.formatter.format(input)
                );
                const expected: TestChunk[] = [{
                    index: 0,
                    data: wholeBuffer.toString(),
                    has_more: false
                }];
                await expect(toArray(gen)).resolves.toEqual(expected);
            });
        });

        describe('when constructed with a large slice', () => {
            let gen: ChunkGenerator;

            let input: Record<string, any>[];

            beforeAll(() => {
                // don't change this count without good reason
                // since it will break the tests
                input = times(100, (index) => ({
                    count: index,
                }));

                gen = new ChunkGenerator(
                    new Formatter({
                        format: Format.ldjson,
                    }),
                    new Compressor(Compression.none),
                    input,
                    undefined,
                    limits
                );
            });

            it('should correctly determine that this can processed per row', () => {
                expect(gen.isRowOptimized()).toBeTrue();
            });

            it('should return a more than one chunk', async () => {
                const str = gen.formatter.format(input);
                // we just need to ensure that our test will work
                expect(str.length).toBeGreaterThan(CHUNK_SIZE);

                const overflow = 6;
                const expected: TestChunk[] = [{
                    index: 0,
                    data: str.slice(0, CHUNK_SIZE + overflow),
                    has_more: true
                },
                {
                    index: 1,
                    data: str.slice(CHUNK_SIZE + overflow, str.length),
                    has_more: false
                }];
                await expect(toArray(gen)).resolves.toEqual(expected);
            });
        });
    });
});

type TestChunk = Overwrite<Chunk, { data: string }>;
async function toArray(gen: ChunkGenerator): Promise<TestChunk[]> {
    const result: TestChunk[] = [];
    for await (const chunk of gen) {
        result.push({
            ...chunk,
            data: chunk.data.toString()
        });
    }
    return result;
}
