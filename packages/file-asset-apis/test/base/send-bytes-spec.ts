import 'jest-extended';
import fs from 'node:fs';
import { join } from 'node:path';
import { debugLogger } from '@terascope/core-utils';
// @ts-expect-error
import fixtures from 'jest-fixtures';
import {
    ChunkGenerator, Formatter, Compressor, Format, Compression, Chunk,
    createFileName, FileSender, ChunkedFileSenderAPIConfig, SendBytes
} from '../../src/index.js';

/**
 * A parquet file is made elsewhere and sent as bytes (sendStream), its
 * records are not formatted here
*/
describe('Parquet files sent as bytes', () => {
    const logger = debugLogger('send-bytes-spec');

    /** Every byte value, which a string round trip would change */
    function binary(size: number, seed = 0): Buffer {
        return Buffer.from(Array.from({ length: size }, (_, i) => (i + seed) % 256));
    }

    async function* stream(...pieces: (Buffer | string)[]): SendBytes {
        for (const piece of pieces) yield piece;
    }

    function bytesGenerator(bytes: SendBytes, chunkBytes: number, compression = Compression.none) {
        return new ChunkGenerator(
            new Formatter({ format: Format.parquet }),
            new Compressor(compression),
            bytes,
            { maxBytes: chunkBytes, minBytes: chunkBytes }
        );
    }

    async function toArray(gen: ChunkGenerator): Promise<Chunk[]> {
        const result: Chunk[] = [];
        for await (const chunk of gen) result.push(chunk);
        return result;
    }

    it('should name a parquet file with its extension', () => {
        expect(createFileName('bucket/tasks/abc', {
            id: '0', filePerSlice: true, sliceCount: 0, format: Format.parquet,
        })).toBe('bucket/tasks/abc/0.0.parquet');
    });

    it('should not format records as parquet', () => {
        const formatter = new Formatter({ format: Format.parquet });
        expect(() => formatter.format([{ a: 1 }])).toThrow('Records cannot be formatted as parquet');
    });

    describe('ChunkGenerator given bytes', () => {
        it('should cut the bytes into chunks of the chunk size, as they arrive', async () => {
            const input = binary(25);
            const chunks = await toArray(bytesGenerator(
                stream(input.subarray(0, 3), input.subarray(3, 17), input.subarray(17)), 10
            ));

            expect(chunks.map(({ index, has_more, data }) => ({
                index, has_more, length: data.length
            }))).toEqual([
                { index: 0, has_more: true, length: 10 },
                { index: 1, has_more: true, length: 10 },
                { index: 2, has_more: false, length: 5 },
            ]);
            expect(Buffer.concat(chunks.map(({ data }) => data as Buffer))).toEqual(input);
        });

        it('should mark the last chunk when the bytes end on a chunk', async () => {
            const input = binary(20);
            const chunks = await toArray(bytesGenerator(stream(input), 10));

            expect(chunks.map(({ has_more }) => has_more)).toEqual([true, false]);
            expect(Buffer.concat(chunks.map(({ data }) => data as Buffer))).toEqual(input);
        });

        it('should give a full chunk as soon as more bytes arrive, holding one chunk', async () => {
            const input = binary(15);
            let sent = 0;
            async function* slow(): SendBytes {
                const pieces = [input.subarray(0, 10), input.subarray(10, 11), input.subarray(11)];
                for (const piece of pieces) {
                    sent++;
                    yield piece;
                }
            }
            const iterator = bytesGenerator(slow(), 10)[Symbol.asyncIterator]();

            const first = await iterator.next();
            // the first chunk is known to have more after the second piece, not the third
            expect(sent).toBe(2);
            expect(first.value).toEqual({ index: 0, has_more: true, data: input.subarray(0, 10) });

            const rest: Chunk[] = [];
            for await (const chunk of { [Symbol.asyncIterator]: () => iterator }) rest.push(chunk);
            expect(rest).toEqual([{ index: 1, has_more: false, data: input.subarray(10) }]);
        });

        it('should cut a piece bigger than a chunk into chunks', async () => {
            const input = binary(35);
            const chunks = await toArray(bytesGenerator(stream(input), 10));

            expect(chunks.map(({ index, has_more, data }) => [index, has_more, data.length]))
                .toEqual([[0, true, 10], [1, true, 10], [2, true, 10], [3, false, 5]]);
            expect(Buffer.concat(chunks.map(({ data }) => data as Buffer))).toEqual(input);
        });

        it('should keep the order and bytes of strings and buffers mixed', async () => {
            const pieces: (Buffer | string)[] = [];
            for (let i = 0; i < 2000; i++) {
                pieces.push(i % 7 === 0 ? Buffer.from([i % 256, 0xff]) : `{"é":${i}}\n`, '');
            }
            const expected = Buffer.concat(pieces.map((p) => Buffer.from(p)));
            const chunks = await toArray(bytesGenerator(stream(...pieces), 1000));

            expect(chunks.every(({ data }, i) => data.length === 1000 || i === chunks.length - 1))
                .toBeTrue();
            expect(Buffer.concat(chunks.map(({ data }) => data as Buffer))).toEqual(expected);
        });

        it('should give one chunk without more for bytes smaller than a chunk', async () => {
            const chunks = await toArray(bytesGenerator(stream(binary(4), ''), 10));

            expect(chunks).toEqual([{ index: 0, has_more: false, data: binary(4) }]);
        });

        it('should give no chunks for no bytes', async () => {
            await expect(toArray(bytesGenerator(stream(), 10))).resolves.toEqual([]);
            await expect(toArray(bytesGenerator(stream(Buffer.alloc(0)), 10))).resolves.toEqual([]);
        });

        it('should not compress bytes', async () => {
            await expect(toArray(bytesGenerator(stream(binary(4)), 10, Compression.gzip)))
                .rejects.toThrow('Sending bytes does not support compression');
        });
    });

    describe('a sender', () => {
        let dir: string;
        let config: ChunkedFileSenderAPIConfig;

        beforeEach(async () => {
            dir = await fixtures.createTempDir();
            config = {
                id: '0', path: dir, format: Format.parquet, file_per_slice: true,
            };
        });

        it('should send a parquet file\'s bytes as they are', async () => {
            const input = binary(3000, 7);
            const sender = new FileSender(config, logger);

            await sender.sendStream(stream(input.subarray(0, 1000), input.subarray(1000)));

            expect(fs.readdirSync(dir)).toEqual(['0.0.parquet']);
            expect(fs.readFileSync(join(dir, '0.0.parquet'))).toEqual(input);
        });

        it('should write no file for no bytes', async () => {
            const sender = new FileSender(config, logger);

            await sender.sendStream(stream());

            expect(fs.readdirSync(dir)).toEqual([]);
        });

        it('should not send records as parquet', async () => {
            const sender = new FileSender(config, logger);

            await expect(sender.send([{ a: 1 }])).rejects.toThrow('Records cannot be formatted as parquet');
            await expect(sender.simpleSend([{ a: 1 }])).rejects.toThrow('Records cannot be formatted as parquet');
        });

        it('should not send compressed bytes', async () => {
            const sender = new FileSender({
                ...config, format: Format.ldjson, compression: Compression.gzip,
            }, logger);

            await expect(sender.sendStream(stream('{"a":1}\n'))).rejects.toThrow('sendStream does not support compression');
        });

        it('should send the bytes of any format, like ldjson lines made elsewhere', async () => {
            const sender = new FileSender({ ...config, format: Format.ldjson }, logger);

            await sender.sendStream(stream('{"a":1}\n', '{"a":2}\n'));

            expect(fs.readFileSync(join(dir, '0.0.ldjson'), 'utf8')).toBe('{"a":1}\n{"a":2}\n');
        });
    });
});
