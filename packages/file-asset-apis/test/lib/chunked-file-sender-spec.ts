import 'jest-extended';
import { DataEntity } from '@terascope/job-components';
import {
    FileSenderType,
    ChunkedSenderConfig,
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
            fileName: string, list: (DataEntity | Record<string, unknown>)[]
        ) {
            this.sentData.set(fileName, list);
        }
    }

    const defaults: Partial<ChunkedSenderConfig> = {
        workerId,
        size: 1000,
        connection: 'default',
        remove_header: true,
        ignore_empty: true,
        extra_args: {},
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

    function makeConfig(config: Partial<ChunkedSenderConfig> = {}): ChunkedSenderConfig {
        return Object.assign({}, defaults, config) as ChunkedSenderConfig;
    }

    it('will set file filePerSlice to true if its a file type and format is JSON', () => {
        const test = new Test(FileSenderType.file, makeConfig({ format: Format.json }));
        expect(test.nameOptions.filePerSlice).toBeTrue();

        const test2 = new Test(FileSenderType.hdfs, makeConfig({ format: Format.json }));
        expect(test2.nameOptions.filePerSlice).toBeFalse();
    });

    it('will set file filePerSlice to true if its a file type and compression is anything but none', () => {
        const test1 = new Test(FileSenderType.file, makeConfig({ compression: Compression.gzip }));
        expect(test1.nameOptions.filePerSlice).toBeTrue();

        const test2 = new Test(FileSenderType.file, makeConfig({ compression: Compression.lz4 }));
        expect(test2.nameOptions.filePerSlice).toBeTrue();

        const test3 = new Test(FileSenderType.file, makeConfig({ compression: Compression.none }));
        expect(test3.nameOptions.filePerSlice).toBeFalse();
    });

    it('will set file filePerSlice to true if its a s3 type', () => {
        const test = new Test(FileSenderType.s3, makeConfig({ format: Format.json }));
        expect(test.nameOptions.filePerSlice).toBeTrue();
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
            // @ts-expect-error

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
            // @ts-expect-error

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
            // @ts-expect-error
            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}`);
            expect(test.verifyCalled).toEqual(true);
        });

        it('can make correct path', async () => {
            const newPath = `${path}/final/dir`;
            const test = new Test(FileSenderType.file, makeConfig());
            // @ts-expect-error
            expect(await test.createFileDestinationName(newPath)).toEqual(`${newPath}/${workerId}`);
            expect(test.verifyCalled).toEqual(true);
        });

        it('can add extensions', async () => {
            const test = new Test(FileSenderType.file, makeConfig({ extension: 'stuff' }));
            // @ts-expect-error
            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}stuff`);
        });

        it('can add slice count', async () => {
            const test = new Test(FileSenderType.file, makeConfig({ file_per_slice: true }));
            // @ts-expect-error

            test.incrementCount();
            // @ts-expect-error
            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.0`);
            // @ts-expect-error

            test.incrementCount();
            // @ts-expect-error
            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.1`);
        });
    });

    describe('hdfs destination names', () => {
        it('can make correct base paths', async () => {
            const test = new Test(FileSenderType.hdfs, makeConfig());
            // @ts-expect-error
            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}`);
            expect(test.verifyCalled).toEqual(true);
        });

        it('can make correct path', async () => {
            const newPath = `${path}/final/dir`;
            const test = new Test(FileSenderType.hdfs, makeConfig());
            // @ts-expect-error
            expect(await test.createFileDestinationName(newPath)).toEqual(`${newPath}/${workerId}`);
            expect(test.verifyCalled).toEqual(true);
        });

        it('can add extensions', async () => {
            const test = new Test(FileSenderType.hdfs, makeConfig({ extension: 'stuff' }));
            // @ts-expect-error
            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}stuff`);
        });

        it('can add slice count', async () => {
            const test = new Test(FileSenderType.hdfs, makeConfig({ file_per_slice: true }));
            // @ts-expect-error

            test.incrementCount();
            // @ts-expect-error
            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.0`);
            // @ts-expect-error

            test.incrementCount();
            // @ts-expect-error
            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.1`);
        });
    });

    describe('s3 destination names', () => {
        it('can make correct base paths', async () => {
            const test = new Test(FileSenderType.s3, makeConfig());
            // @ts-expect-error

            test.incrementCount();
            // @ts-expect-error
            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.0`);
            expect(test.verifyCalled).toEqual(false);
        });

        it('can make correct path', async () => {
            const newPath = `${path}/final/dir`;
            const test = new Test(FileSenderType.s3, makeConfig());
            // @ts-expect-error

            test.incrementCount();
            // @ts-expect-error
            expect(await test.createFileDestinationName(newPath)).toEqual(`${newPath}/${workerId}.0`);
            expect(test.verifyCalled).toEqual(false);
        });

        it('can add extensions', async () => {
            const test = new Test(FileSenderType.s3, makeConfig({ extension: 'stuff' }));
            // @ts-expect-error

            test.incrementCount();
            // @ts-expect-error
            expect(await test.createFileDestinationName(path)).toEqual(`${path}/${workerId}.0stuff`);
        });
    });

    it('can prepare a segment for sending', async () => {
        const test = new Test(FileSenderType.s3, makeConfig());
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

        expect(results.fileName).toEqual(`${path}/${workerId}.0`);
        expect(results.output).toEqual('{"some":"data"}\n{"other":"stuff"}\n');
    });
});
