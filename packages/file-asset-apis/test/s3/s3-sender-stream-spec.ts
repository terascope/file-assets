import 'jest-extended';
import { debugLogger, pDelay } from '@terascope/core-utils';
import {
    Format, S3Sender, SendBytes, MIN_CHUNK_SIZE_BYTES, STREAM_CHUNK_SIZE_BYTES, MiB
} from '../../src/index.js';

/**
 * sendStream uploads a file's bytes in parts of 8MiB by default, a part
 * size which keeps the bytes held small
*/
describe('S3 Sender sendStream parts', () => {
    const logger = debugLogger('s3-sender-stream');

    it('should upload 8MiB parts by default, the last smaller', async () => {
        const parts: number[] = [];
        const client = {
            async send(command: { constructor: { name: string }; input: any }) {
                const name = command.constructor.name;
                if (name === 'CreateMultipartUploadCommand') return { UploadId: 'upload-1' };
                if (name === 'UploadPartCommand') {
                    parts[command.input.PartNumber - 1] = command.input.Body.length;
                    return { ETag: `etag-${command.input.PartNumber}` };
                }
                return {};
            }
        };
        async function* bytes(): SendBytes {
            for (let i = 0; i < 20; i++) yield Buffer.alloc(MiB, i);
        }
        const sender = new S3Sender(client as any, {
            id: '0', path: 'bucket/key', format: Format.parquet, file_per_slice: true,
        }, logger);

        await sender.sendStream(bytes());

        expect(STREAM_CHUNK_SIZE_BYTES).toBe(8 * MiB);
        expect(parts).toEqual([8 * MiB, 8 * MiB, 4 * MiB]);
    });
});

/**
 * A multipart upload whose part fails stops at it: it rejects with the
 * part's error, aborts the upload and stops reading what it sends, with no
 * unhandled rejection left behind (that ends the process)
*/
describe('S3 Sender, a part upload that fails', () => {
    const logger = debugLogger('s3-sender-part-error');
    const chunkSize = MIN_CHUNK_SIZE_BYTES;
    let unhandled: unknown[];
    const onUnhandled = (err: unknown) => {
        unhandled.push(err);
    };

    beforeEach(() => {
        unhandled = [];
        process.on('unhandledRejection', onUnhandled);
    });

    afterEach(() => {
        process.off('unhandledRejection', onUnhandled);
    });

    /** An S3 client whose part uploads fail */
    function makeFailingClient() {
        const commands: string[] = [];
        const client = {
            async send(command: object) {
                const name = command.constructor.name;
                commands.push(name);
                if (name === 'CreateMultipartUploadCommand') return { UploadId: 'upload-1' };
                if (name === 'UploadPartCommand') {
                    await pDelay(5);
                    throw new Error('part upload failed');
                }
                return {};
            }
        };
        return { client: client as any, commands };
    }

    function makeBytes(chunks: number) {
        const state = { pulled: 0, returned: false };
        async function* bytes(): SendBytes {
            try {
                for (let i = 0; i < chunks; i++) {
                    state.pulled++;
                    yield Buffer.alloc(chunkSize, i);
                }
            } finally {
                state.returned = true;
            }
        }
        return { bytes: bytes(), state };
    }

    it('should reject sendStream with the part\'s error and stop reading the bytes', async () => {
        const { client, commands } = makeFailingClient();
        const { bytes, state } = makeBytes(40);
        const sender = new S3Sender(client, {
            id: '0', path: 'bucket/key', format: Format.parquet, file_per_slice: true, concurrency: 2,
        }, logger);

        await expect(sender.sendStream(bytes, { chunkBytes: chunkSize }))
            .rejects.toThrow('part upload failed');
        await pDelay(50);

        expect(state.pulled).toBeLessThan(10);
        expect(state.returned).toBeTrue();
        expect(commands).toContain('AbortMultipartUploadCommand');
        expect(commands).not.toContain('CompleteMultipartUploadCommand');
        expect(unhandled).toEqual([]);
    });

    it('should reject simpleSend with the part\'s error, without unhandled rejections', async () => {
        const { client, commands } = makeFailingClient();
        // enough ldjson for several chunks
        const record = { text: 'x'.repeat(1024 * 1024) };
        const records = Array.from({ length: 40 }, () => record);
        const sender = new S3Sender(client, {
            id: '0', path: 'bucket/key', format: Format.ldjson, file_per_slice: true, concurrency: 2,
        }, logger);

        await expect(sender.simpleSend(records)).rejects.toThrow('part upload failed');
        await pDelay(50);

        expect(commands).toContain('AbortMultipartUploadCommand');
        expect(commands).not.toContain('CompleteMultipartUploadCommand');
        expect(unhandled).toEqual([]);
    });
});
