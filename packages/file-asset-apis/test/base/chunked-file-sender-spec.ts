import 'jest-extended';
import { DataEntity } from '@terascope/utils';
import {
    FileSenderType,
    ChunkedFileSenderConfig,
    Compression,
    Format,
    ChunkedFileSender,
    SendBatchConfig
} from '../../src';

describe('ChunkedSlicer', () => {
    const path = 'some/path';
    const workerId = '1234';

    class Test extends ChunkedFileSender {
        sentData = new Map();
        verifyCalled = false;

        async verify() {
            this.verifyCalled = true;
        }

        testJoinPath(pathing?: string) {
            return this.joinPath(pathing);
        }

        async sendToDestination(
            { dest, chunkGenerator }: SendBatchConfig
        ) {
            let output: Buffer|undefined;

            for await (const chunk of chunkGenerator) {
                if (chunk.has_more) {
                    throw new Error('has_more is not supported');
                }
                output = chunk.data;
            }

            if (output) {
                this.sentData.set(dest, output.toString());
            }
        }
    }

    const defaults: Partial<ChunkedFileSenderConfig> = {
        id: workerId,
        path
    };

    function makeConfig(
        format: Format,
        config: Partial<Omit<ChunkedFileSenderConfig, 'format'>> = {}
    ): ChunkedFileSenderConfig {
        return Object.assign({}, defaults, config, { format }) as ChunkedFileSenderConfig;
    }

    it('will throw if file_per_slice is false if its a file type and format is JSON', () => {
        const errMsg = 'Invalid parameter "file_per_slice", it must be set to true if format is set to json';
        expect(
            () => new Test(FileSenderType.file, makeConfig(Format.json))
        ).toThrowError(errMsg);
    });

    it('will throw if file_per_slice is false and compression is anything but none', () => {
        const errMsg = 'Invalid parameter "file_per_slice", it must be set to true if compression is set to anything other than "none" as we cannot properly divide up a compressed file';
        expect(() => {
            const config = makeConfig(Format.ldjson, { compression: Compression.gzip });
            new Test(FileSenderType.file, config);
        }).toThrowError(errMsg);

        expect(() => {
            const config = makeConfig(Format.ldjson, { compression: Compression.lz4 });
            new Test(FileSenderType.file, config);
        }).toThrowError(errMsg);

        const test3 = new Test(FileSenderType.file, makeConfig(
            Format.ldjson, { compression: Compression.none }
        ));
        expect(test3.nameOptions.filePerSlice).toBeFalse();
    });

    it('can check its a router is being used', () => {
        const test = new Test(FileSenderType.file, makeConfig(
            Format.ldjson, { dynamic_routing: true }
        ));
        expect(test.isRouter).toBeTrue();

        const test2 = new Test(FileSenderType.file, makeConfig(
            Format.ldjson
        ));
        expect(test2.isRouter).toBeFalse();
    });

    it('can prepare a dispatch with no routing', async () => {
        const test = new Test(FileSenderType.file, makeConfig(Format.ldjson));
        const data = [
            DataEntity.make({ name: 'chilly' }),
            DataEntity.make({ name: 'willy' }, { 'standard:route': 'a' }),
            DataEntity.make({ name: 'billy' }, { 'standard:route': 'b' }),
        ];

        const results = await test.prepareDispatch(data);
        expect(results).toBeArrayOfSize(1);

        const [firstBatchConfig] = results;

        expect(firstBatchConfig).toHaveProperty('filename', path);
        expect(firstBatchConfig).toHaveProperty('chunkGenerator');
        expect(firstBatchConfig.chunkGenerator.slice).toBeArrayOfSize(3);
    });

    it('can prepare a dispatch with routing', async () => {
        const test = new Test(FileSenderType.file, makeConfig(
            Format.ldjson, { dynamic_routing: true }
        ));
        const data = [
            DataEntity.make({ name: 'chilly' }),
            DataEntity.make({ name: 'willy' }, { 'standard:route': 'a' }),
            DataEntity.make({ name: 'billy' }, { 'standard:route': 'b' }),
        ];

        const results = await test.prepareDispatch(data);

        expect(results).toBeArrayOfSize(3);

        const [firstBatchConfig, secondBatchConfig, thirdBatchConfig] = results;

        expect(firstBatchConfig).toHaveProperty('filename', `${path}`);
        expect(firstBatchConfig).toHaveProperty('chunkGenerator');
        expect(firstBatchConfig.chunkGenerator.slice).toBeArrayOfSize(1);

        expect(secondBatchConfig).toHaveProperty('filename', `${path}/a`);
        expect(secondBatchConfig).toHaveProperty('chunkGenerator');
        expect(secondBatchConfig.chunkGenerator.slice).toBeArrayOfSize(1);

        expect(thirdBatchConfig).toHaveProperty('filename', `${path}/b`);
        expect(thirdBatchConfig).toHaveProperty('chunkGenerator');
        expect(thirdBatchConfig.chunkGenerator.slice).toBeArrayOfSize(1);
    });

    it('can make a new route path', () => {
        const test = new Test(FileSenderType.file, makeConfig(Format.ldjson));

        expect(test.testJoinPath()).toEqual(path);
        expect(test.testJoinPath(path)).toEqual(path);
        expect(test.testJoinPath('last/part')).toEqual(`${path}/last/part`);
    });

    describe('file destination names', () => {
        it('can make correct base paths', async () => {
            const test = new Test(FileSenderType.file, makeConfig(Format.ldjson));

            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.ldjson`);
            expect(test.verifyCalled).toEqual(true);
        });

        it('can make correct path', async () => {
            const newPath = `${path}/final/dir`;
            const test = new Test(FileSenderType.file, makeConfig(Format.ldjson));

            expect(await test.createFileDestinationName(newPath)).toEqual(`${newPath}/${workerId}.ldjson`);
            expect(test.verifyCalled).toEqual(true);
        });

        it('can add extensions', async () => {
            const test = new Test(FileSenderType.file, makeConfig(
                Format.ldjson, { extension: 'stuff' }
            ));
            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.stuff`);
        });

        it('can add slice count', async () => {
            const test = new Test(FileSenderType.file, makeConfig(
                Format.ldjson, { file_per_slice: true }
            ));
            // @ts-expect-error
            test.incrementCount();

            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.0.ldjson`);
            // @ts-expect-error
            test.incrementCount();

            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.1.ldjson`);
        });

        it('can add extensions and file_per_slice', async () => {
            const test = new Test(FileSenderType.file, makeConfig(
                Format.ldjson, { file_per_slice: true, extension: 'stuff' }
            ));
            // @ts-expect-error
            test.incrementCount();
            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.0.stuff`);
        });

        it('can respect compression and other formats', async () => {
            const test = new Test(
                FileSenderType.file,
                makeConfig(Format.ldjson, { compression: Compression.lz4, file_per_slice: true })
            );
            // @ts-expect-error
            test.incrementCount();
            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.0.ldjson.lz4`);

            const test2 = new Test(
                FileSenderType.file,
                makeConfig(Format.csv, {
                    compression: Compression.gzip,
                    file_per_slice: true
                })
            );
            // @ts-expect-error
            test2.incrementCount();
            expect(await test2.createFileDestinationName(path)).toEqual(`${path}/${workerId}.0.csv.gz`);

            const test3 = new Test(
                FileSenderType.file,
                makeConfig(Format.json, {
                    compression: Compression.none,
                    file_per_slice: true
                })
            );
            // @ts-expect-error
            test3.incrementCount();
            expect(await test3.createFileDestinationName(path)).toEqual(`${path}/${workerId}.0.json`);
        });
    });

    describe('hdfs destination names', () => {
        it('can make correct base paths', async () => {
            const test = new Test(FileSenderType.hdfs, makeConfig(Format.ldjson));

            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.ldjson`);
            expect(test.verifyCalled).toEqual(true);
        });

        it('can make correct path', async () => {
            const newPath = `${path}/final/dir`;
            const test = new Test(FileSenderType.hdfs, makeConfig(Format.ldjson));

            expect(await test.createFileDestinationName(newPath)).toEqual(`${newPath}/${workerId}.ldjson`);
            expect(test.verifyCalled).toEqual(true);
        });

        it('can add extensions', async () => {
            const test = new Test(FileSenderType.hdfs, makeConfig(
                Format.ldjson, { extension: 'stuff' }
            ));

            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.stuff`);
        });

        it('can add slice count', async () => {
            const test = new Test(FileSenderType.hdfs, makeConfig(
                Format.ldjson, { file_per_slice: true }
            ));

            // @ts-expect-error
            test.incrementCount();

            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.0.ldjson`);
            // @ts-expect-error
            test.incrementCount();

            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.1.ldjson`);
        });
    });

    describe('s3 destination names', () => {
        it('can make correct base paths', async () => {
            const test = new Test(FileSenderType.s3, makeConfig(
                Format.ldjson, { file_per_slice: true }
            ));
            // @ts-expect-error
            test.incrementCount();

            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.0.ldjson`);
            expect(test.verifyCalled).toEqual(false);
        });

        it('can make correct path', async () => {
            const newPath = `${path}/final/dir`;
            const test = new Test(FileSenderType.s3, makeConfig(
                Format.ldjson, { file_per_slice: true }
            ));
            // @ts-expect-error
            test.incrementCount();

            expect(await test.createFileDestinationName(newPath)).toEqual(`${newPath}/${workerId}.0.ldjson`);
            expect(test.verifyCalled).toEqual(false);
        });

        it('can add extensions', async () => {
            const test = new Test(FileSenderType.s3, makeConfig(
                Format.ldjson, { extension: 'stuff', file_per_slice: true }
            ));
            // @ts-expect-error
            test.incrementCount();

            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.0.stuff`);
        });
    });

    // it('can prepare a segment for sending', async () => {
    //     const test = new Test(FileSenderType.s3, makeConfig(
    //         Format.ldjson, { file_per_slice: true }
    //     ));
    //     const records = [
    //         DataEntity.make({ some: 'data' }),
    //         DataEntity.make({ other: 'stuff' }),
    //     ];
    //     test.incrementCount();

    //     const results = await test.prepareSegment(path, records);

    //     expect(results).toBeDefined();
    //     expect(results.fileName).toBeDefined();
    //     expect(results.output).toBeDefined();

    //     expect(results.fileName).toEqual(`${path}/${workerId}.0.ldjson`);
    //     expect(results.output?.toString()).toEqual('{"some":"data"}\n{"other":"stuff"}\n');
    // });

    it('can respect file destination using send and field_per_slice false', async () => {
        const test = new Test(
            FileSenderType.s3,
            makeConfig(Format.ldjson, {
                file_per_slice: true,
                compression: Compression.none,
                extension: '.ldjson'
            })
        );

        const data = [{ some: 'data' }, { other: 'data' }];
        const expectedTransforms = '{"some":"data"}\n{"other":"data"}\n';

        await test.send(data);
        await test.send(data);

        const results = Array.from(test.sentData);

        expect(results).toBeArrayOfSize(2);

        const [firstPath, firstRecords] = results[0];
        const [secondPath, secondRecords] = results[1];

        expect(firstPath).toEqual('some/path/1234.0.ldjson');
        expect(firstRecords).toEqual(expectedTransforms);

        expect(secondPath).toEqual('some/path/1234.1.ldjson');
        expect(secondRecords).toEqual(expectedTransforms);
    });
});
