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
        let statePath;
        let decompress;
        let ready;
        let archive;
        let filedb;
        it('should die with invalid saved state', async () => {
            uploadDir = await fixtures.copyFixtureIntoTempDir(__dirname, 't1');
            upload = path.join(uploadDir, file);
            workDir = await fixtures.createTempDir();
            statePath = path.join(workDir, 'compressed_file_reader.json');
            decompress = path.join(workDir, 'decompress', file);
            ready = path.join(workDir, 'ready', file);
            archive = path.join(workDir, 'archive', file);
            await fse.writeFile(statePath, 'Hi, Mom!');
            await expect(FileDB(workDir, path.resolve('asset'))).rejects.toThrow();
        });
        it('should be able to initialize with no saved statePath', async () => {
            await fse.unlink(statePath);
            filedb = await FileDB(workDir, path.resolve('asset'));
        });
        it('should decompress to a ready working dir', async () => {
            expect(await filedb.ready(upload)).toEqual(ready);
            expect(await fse.exists(decompress)).toBeFalse();
            expect(await fse.exists(ready)).toBeTrue();
            expect(await fse.exists(upload)).toBeTrue();
        });
        it('should write its state to disk', async () => {
            const state = await fse.readJson(path.join(workDir, 'compressed_file_reader.json'));
            expect(state[upload].src).toEqual(upload);
            expect(state[upload].name).toEqual(file);
            expect(state[upload].stage).toEqual('ready');
        });
        it('should be able to archive files', async () => {
            expect(await filedb.archive(upload)).toEqual(archive);
            expect(await fse.exists(ready)).toBeFalse();
            expect(await fse.exists(archive)).toBeTrue();
            expect(await fse.exists(upload)).toBeFalse();
        });
        it('should remove a file from its state once archived', async () => {
            const state = await fse.readJson(path.join(workDir, 'compressed_file_reader.json'));
            expect(state[upload]).toBeUndefined();
            fixtures.cleanupTempDirs();
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
            ready = path.join(workDir, 'ready', file);
            archive = path.join(workDir, 'archive', file);
            filedb = await FileDB(workDir, path.resolve('asset'));
        });
        it('should fail to decompress', async () => {
            process.env.TEST_EXIT_CODE = 42;
            let code;
            try {
                await filedb.ready(upload);
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
            expect(await filedb.ready(upload)).toEqual(ready);
            expect(await fse.exists(decompress)).toBeFalse();
            expect(await fse.exists(ready)).toBeTrue();
            expect(await fse.exists(upload)).toBeTrue();
        });
        it('should write its state to disk', async () => {
            const state = await fse.readJson(path.join(workDir, 'compressed_file_reader.json'));
            expect(state[upload].src).toEqual(upload);
            expect(state[upload].name).toEqual(file);
            expect(state[upload].stage).toEqual('ready');
        });
        it('should be able to archive files', async () => {
            expect(await filedb.archive(upload)).toEqual(archive);
            expect(await fse.exists(ready)).toBeFalse();
            expect(await fse.exists(archive)).toBeTrue();
            expect(await fse.exists(upload)).toBeFalse();
        });
        it('should remove a file from its state once archived', async () => {
            const state = await fse.readJson(path.join(workDir, 'compressed_file_reader.json'));
            expect(state[upload]).toBeUndefined();
            fixtures.cleanupTempDirs();
        });
    });
    // TODO: Fail to write state.
});
