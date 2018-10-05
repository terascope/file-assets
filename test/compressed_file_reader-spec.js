'use strict';

const FileDB = require('../asset/compressed_file_reader/filedb');
const fixtures = require('jest-fixtures');
const fse = require('fs-extra');
const path = require('path');

describe('The compressed_file_reader', () => {
    describe('when single file uploaded', () => {
        const file = 'package.json.lz4';
        let uploadDir;
        let upload;
        let workDir;
        let decompress;
        let ready;
        let archive;
        let filedb;
        it('should be able to initialize', async () => {
            uploadDir = await fixtures.copyFixtureIntoTempDir(__dirname, 't1');
            upload = path.join(uploadDir, file);
            workDir = await fixtures.createTempDir();
            decompress = path.join(workDir, 'decompress', file);
            ready = path.join(workDir, 'slice', file);
            archive = path.join(workDir, 'archive', file);
            filedb = await FileDB(workDir, path.resolve('asset'));
        });
        it('waits for client to mark file as ready', async () => {
            expect(filedb.numReady()).toEqual(0);
            expect(await filedb.uploaded(upload)).toEqual(false);
            expect(filedb.enqueue()).toEqual(null);
            await fse.writeFile(`${upload}.ready`, 'Hi, Mom!');
            expect(await filedb.uploaded(upload)).toEqual(true);
            expect(filedb.numReady()).toEqual(1);
            expect(filedb.enqueue()).toEqual(upload);
        });
        it('should decompress to a ready-for-slicing work dir', async () => {
            expect(await filedb.decompress(upload)).toEqual(ready);
            expect(await fse.exists(decompress)).toBeFalse();
            expect(await fse.exists(ready)).toBeTrue();
            expect(await fse.exists(upload)).toBeTrue();
        });
        it('should be able to archive files', async () => {
            expect(await filedb.archive(upload)).toEqual(archive);
            expect(await fse.exists(ready)).toBeFalse();
            expect(await fse.exists(archive)).toBeTrue();
            expect(await fse.exists(upload)).toBeFalse();
        });
    });

    describe('when failed to decompress', () => {
        const file = 'package.json.lz4';
        let uploadDir;
        let upload;
        let workDir;
        let decompress;
        let ready;
        let archive;
        let filedb;
        it('should be able to initialize', async () => {
            uploadDir = await fixtures.copyFixtureIntoTempDir(__dirname, 't1');
            upload = path.join(uploadDir, file);
            workDir = await fixtures.createTempDir();
            decompress = path.join(workDir, 'decompress', file);
            ready = path.join(workDir, 'slice', file);
            archive = path.join(workDir, 'archive', file);
            filedb = await FileDB(workDir, path.resolve('asset'));
        });
        it('should fail to decompress', async () => {
            await fse.writeFile(`${upload}.ready`, 'Hi, Mom!');
            expect(await filedb.uploaded(upload)).toEqual(true);
            expect(filedb.enqueue()).toEqual(upload);
            process.env.TEST_EXIT_CODE = 42;
            let code;
            try {
                await filedb.decompress(upload);
            } catch (err) {
                code = err.code;
            }
            expect(code).toEqual(42);
            expect(await fse.exists(decompress)).toBeFalse();
            expect(await fse.exists(ready)).toBeFalse();
            expect(await fse.exists(upload)).toBeTrue();
        });
        it('should recover automatically on next attempt', async () => {
            delete process.env.TEST_EXIT_CODE;
            filedb = await FileDB(workDir, path.resolve('asset'));
            await fse.writeFile(`${upload}.ready`, 'Hi, Mom!');
            expect(await filedb.uploaded(upload)).toEqual(true);
            expect(filedb.enqueue()).toEqual(upload);
            expect(await filedb.decompress(upload)).toEqual(ready);
            expect(await fse.exists(decompress)).toBeFalse();
            expect(await fse.exists(ready)).toBeTrue();
            expect(await fse.exists(upload)).toBeTrue();
        });
    });

    // TODO: filedb cases
    // - partially archived (eg rename fails)
    // TODO: slicer cases
    // - readied but not sliced
    // - one file fails in onUpstreamGlobbed
    // - file not decompressed before next glob cycle
});
