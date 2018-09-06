'use strict';

// The file mgmt heavy lifting of the compressed file reader. Try to keep this
// pure - defer things like error handling & logging to the caller.

const path = require('path');
const fse = require('fs-extra');
const _ = require('lodash');
const { spawn } = require('child_process');

// Enumerate stages that a file can be in.
const STAGE = {
    upload: 'upload',
    decompress: 'decompress',
    ready: 'ready',
    archive: 'archive',
};

module.exports = async (workDir, assetDir) => {
    let dirty = false;
    let state = {};

    const workPath = _.partial(path.join, workDir);
    const dbPath = workPath('compressed_file_reader.json');

    if (await fse.exists(dbPath)) {
        state = await fse.readJson(dbPath);
    }
    await fse.mkdirs(workPath('decompress'));
    await fse.mkdirs(workPath('ready'));
    await fse.mkdirs(workPath('archive'));

    async function write() {
        // Only write when dirty.  Mark as clean early to avoid concurrent writing.
        if (dirty) {
            dirty = false;
            try {
                await fse.writeJson(dbPath, state, { spaces: 2 });
            } catch (err) {
                dirty = true;
                throw err;
            }
        }
    }

    async function archive(src) {
        const { name } = state[src];
        const archivePath = workPath('archive', name);
        await fse.rename(src, archivePath);
        await fse.unlink(workPath('ready', name));
        delete state[src];
        dirty = true;
        await write();
        return archivePath;
    }

    // Get uploaded file ready for slicing, returning [original src path, decompressed ready path]
    async function ready(src) {
        if (!state[src]) {
            state[src] = {
                src,
                name: path.basename(src),
                stage: STAGE.upload,
            };
            dirty = true;
        }
        const data = state[src];
        const decompressPath = workPath('decompress', data.name);
        if (data.stage === STAGE.upload) {
            const { code, stderr } = await decompress(src, decompressPath);
            if (code > 0) {
                throw new Error(`failed to decompress ${src}: ${stderr}`);
            }
            data.stage = STAGE.decompress;
            dirty = true;
        }
        const readyPath = workPath('ready', data.name);
        if (data.stage === STAGE.decompress) {
            await fse.rename(decompressPath, readyPath);
            data.stage = STAGE.ready;
            dirty = true;
        }
        write();
        return readyPath;
    }

    // Decompress file in a sub-shell to keep event loop free. Exit code,
    // stdout, & stderr return - error handling pushed to the caller.
    async function decompress(src, dst) {
        const proc = spawn(`${assetDir}/bin/decompress`, [src, dst], {});
        let stderr = '';
        proc.stderr.on('data', (data) => {
            stderr += data;
        });
        let stdout = '';
        proc.stdout.on('data', (data) => {
            stdout += data;
        });
        return new Promise((resolve) => {
            proc.on('close', (code) => {
                resolve({
                    code, stdout, stderr, src, dst
                });
            });
        });
    }

    return {
        ready,
        archive,
    };
};
