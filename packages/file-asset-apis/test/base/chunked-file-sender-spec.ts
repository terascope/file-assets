import 'jest-extended';
import { DataEntity } from '@terascope/utils';
import {
    FileSenderType,
    BaseSenderConfig,
    Compression,
    Format,
    ChunkedFileSender
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
            file: string, list: (DataEntity | Record<string, unknown>)[]
        ) {
            const { fileName, output } = await this.prepareSegment(file, list);
            this.sentData.set(fileName, output.toString());
        }
    }

    const defaults: Partial<BaseSenderConfig> = {
        id: workerId,
        compression: Compression.none,
        field_delimiter: ',',
        line_delimiter: '\n',
        fields: [],
        file_per_slice: false,
        include_header: false,
        format: Format.ldjson,
        path,
        dynamic_routing: false
    };

    function makeConfig(config: Partial<BaseSenderConfig> = {}): BaseSenderConfig {
        return Object.assign({}, defaults, config) as BaseSenderConfig;
    }

    it('will throw if file_per_slice is false if its a file type and format is JSON', () => {
        const errMsg = 'Invalid parameter "file_per_slice", it must be set to true if format is set to json';
        expect(
            () => new Test(FileSenderType.file, makeConfig({ format: Format.json }))
        ).toThrowError(errMsg);
    });

    it('will throw if file_per_slice is false and compression is anything but none', () => {
        const errMsg = 'Invalid parameter "file_per_slice", it must be set to true if compression is set to anything other than "none" as we cannot properly divide up a compressed file';
        expect(
            () => new Test(FileSenderType.file, makeConfig({ compression: Compression.gzip }))
        ).toThrowError(errMsg);

        expect(
            () => new Test(FileSenderType.file, makeConfig({ compression: Compression.lz4 }))
        ).toThrowError(errMsg);

        const test3 = new Test(FileSenderType.file, makeConfig({ compression: Compression.none }));
        expect(test3.nameOptions.filePerSlice).toBeFalse();
    });

    it('can check its a router is being used', () => {
        const test = new Test(FileSenderType.file, makeConfig({ dynamic_routing: true }));
        expect(test.isRouter).toBeTrue();

        const test2 = new Test(FileSenderType.file, makeConfig());
        expect(test2.isRouter).toBeFalse();
    });

    it('can prepare a dispatch with no routing', () => {
        const test = new Test(FileSenderType.file, makeConfig());
        const data = [
            DataEntity.make({ name: 'chilly' }),
            DataEntity.make({ name: 'willy' }, { 'standard:route': 'a' }),
            DataEntity.make({ name: 'billy' }, { 'standard:route': 'b' }),
        ];

        const results = test.prepareDispatch(data);
        expect(results[path]).toBeArrayOfSize(3);
    });

    it('can prepare a dispatch with routing', () => {
        const test = new Test(FileSenderType.file, makeConfig({ dynamic_routing: true }));
        const data = [
            DataEntity.make({ name: 'chilly' }),
            DataEntity.make({ name: 'willy' }, { 'standard:route': 'a' }),
            DataEntity.make({ name: 'billy' }, { 'standard:route': 'b' }),
        ];

        const results = test.prepareDispatch(data);

        expect(results[path]).toBeArrayOfSize(1);
        expect(results[`${path}/a`]).toBeArrayOfSize(1);
        expect(results[`${path}/b`]).toBeArrayOfSize(1);
    });

    it('can make a new route path', () => {
        const test = new Test(FileSenderType.file, makeConfig());

        expect(test.testJoinPath()).toEqual(path);
        expect(test.testJoinPath(path)).toEqual(path);
        expect(test.testJoinPath('last/part')).toEqual(`${path}/last/part`);
    });

    describe('file destination names', () => {
        it('can make correct base paths', async () => {
            const test = new Test(FileSenderType.file, makeConfig());

            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.ldjson`);
            expect(test.verifyCalled).toEqual(true);
        });

        it('can make correct path', async () => {
            const newPath = `${path}/final/dir`;
            const test = new Test(FileSenderType.file, makeConfig());

            expect(await test.createFileDestinationName(newPath)).toEqual(`${newPath}/${workerId}.ldjson`);
            expect(test.verifyCalled).toEqual(true);
        });

        it('can add extensions', async () => {
            const test = new Test(FileSenderType.file, makeConfig({ extension: 'stuff' }));
            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.stuff`);
        });

        it('can add slice count', async () => {
            const test = new Test(FileSenderType.file, makeConfig({ file_per_slice: true }));
            // @ts-expect-error
            test.incrementCount();

            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.0.ldjson`);
            // @ts-expect-error
            test.incrementCount();

            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.1.ldjson`);
        });

        it('can add extensions and file_per_slice', async () => {
            const test = new Test(FileSenderType.file, makeConfig({ file_per_slice: true, extension: 'stuff' }));
            // @ts-expect-error
            test.incrementCount();
            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.0.stuff`);
        });

        it('can respect compression and other formats', async () => {
            const test = new Test(
                FileSenderType.file,
                makeConfig({ compression: Compression.lz4, file_per_slice: true })
            );
            // @ts-expect-error
            test.incrementCount();
            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.0.ldjson.lz4`);

            const test2 = new Test(
                FileSenderType.file,
                makeConfig({
                    compression: Compression.gzip,
                    format: Format.csv,
                    file_per_slice: true
                })
            );
            // @ts-expect-error
            test2.incrementCount();
            expect(await test2.createFileDestinationName(path)).toEqual(`${path}/${workerId}.0.csv.gz`);

            const test3 = new Test(
                FileSenderType.file,
                makeConfig({
                    compression: Compression.none,
                    format: Format.json,
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
            const test = new Test(FileSenderType.hdfs, makeConfig());

            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.ldjson`);
            expect(test.verifyCalled).toEqual(true);
        });

        it('can make correct path', async () => {
            const newPath = `${path}/final/dir`;
            const test = new Test(FileSenderType.hdfs, makeConfig());

            expect(await test.createFileDestinationName(newPath)).toEqual(`${newPath}/${workerId}.ldjson`);
            expect(test.verifyCalled).toEqual(true);
        });

        it('can add extensions', async () => {
            const test = new Test(FileSenderType.hdfs, makeConfig({ extension: 'stuff' }));

            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.stuff`);
        });

        it('can add slice count', async () => {
            const test = new Test(FileSenderType.hdfs, makeConfig({ file_per_slice: true }));
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
            const test = new Test(FileSenderType.s3, makeConfig({ file_per_slice: true }));
            // @ts-expect-error
            test.incrementCount();

            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.0.ldjson`);
            expect(test.verifyCalled).toEqual(false);
        });

        it('can make correct path', async () => {
            const newPath = `${path}/final/dir`;
            const test = new Test(FileSenderType.s3, makeConfig({ file_per_slice: true }));
            // @ts-expect-error
            test.incrementCount();

            expect(await test.createFileDestinationName(newPath)).toEqual(`${newPath}/${workerId}.0.ldjson`);
            expect(test.verifyCalled).toEqual(false);
        });

        it('can add extensions', async () => {
            const test = new Test(FileSenderType.s3, makeConfig({ extension: 'stuff', file_per_slice: true }));
            // @ts-expect-error
            test.incrementCount();

            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.0.stuff`);
        });
    });

    it('can prepare a segment for sending', async () => {
        const test = new Test(FileSenderType.s3, makeConfig({ file_per_slice: true }));
        const records = [
            DataEntity.make({ some: 'data' }),
            DataEntity.make({ other: 'stuff' }),
        ];
        // @ts-expect-error
        test.incrementCount();

        const results = await test.prepareSegment(path, records);

        expect(results).toBeDefined();
        expect(results.fileName).toBeDefined();
        expect(results.output).toBeDefined();

        expect(results.fileName).toEqual(`${path}/${workerId}.0.ldjson`);
        expect((results.output as Buffer).toString()).toEqual('{"some":"data"}\n{"other":"stuff"}\n');
    });

    it('can respect file destination using send and field_per_slice false', async () => {
        const test = new Test(
            FileSenderType.s3,
            makeConfig({
                file_per_slice: true,
                format: Format.ldjson,
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
