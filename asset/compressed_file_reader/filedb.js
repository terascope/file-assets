'use strict';

// The file mgmt heavy lifting of the compressed file reader. Try to keep this
// pure - defer things like error handling & logging to the caller.

const path = require('path');
const fse = require('fs-extra');
const _ = require('lodash');
const { spawn } = require('child_process');

// Enumerate stages that a file can be in, in this order. The ready & queue
// stages are used so that clients can manage how many files are being
// decompressed concurrently. The transition from decompressed to slice is a
// simple mv/rename after the file has been decompressed. The slice copy is what
// workers will read from.
const STAGE = {
    uploading: 'uploading',
    ready: 'ready',
    queue: 'queue',
    decompressed: 'decompressed',
    slice: 'slice',
    archived: 'archived',
};

module.exports = async (workDir, assetDir) => {
    let dirty = false;
    const state = {};

    const workPath = _.partial(path.join, workDir);
    const dbPath = workPath('compressed_file_reader.json');

    await fse.mkdirs(workPath('decompress'));
    await fse.mkdirs(workPath('slice'));
    await fse.mkdirs(workPath('archive'));

    // The original idea of this state file was to progress a file from its last
    // successful stage. That was making the crash-proof story too complicated
    // and all it would really save us is extra decompression. Instead, if a
    // file fails to get processed, this reader should take it from ready to
    // archive on restart, assuming the error was transient (eg volume full).
    //
    // Leaving the ability to dump its state to disk as a crude but simple method
    // for operators to inspect current state.
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
    if (process.env.NODE_ENV !== 'test') {
        setInterval(write, 1000);
    }

    // TODO: Include functionality to purge archives after configurable amount
    // of time or space available.
    async function archive(src) {
        const { name } = state[src];
        const archivePath = workPath('archive', name);
        await fse.rename(src, archivePath);
        await fse.unlink(`${src}.ready`);
        await fse.unlink(workPath('slice', name));
        delete state[src];
        dirty = true;
        return archivePath;
    }

    // True if completely & newly uploaded. Once uploaded, the file will be
    // marked as ready for enqueue() and subsequent calls to this function will
    // be false.
    async function uploaded(src) {
        if (!state[src]) {
            state[src] = {
                src,
                name: path.basename(src),
                stage: STAGE.uploading,
            };
        }
        if (state[src].stage === STAGE.uploading && await fse.exists(`${src}.ready`)) {
            state[src].stage = STAGE.ready;
            dirty = true;
            return true;
        }
        return false;
    }

    // Return src file available for work, null if none ready. Internal state
    // updated so same file won't be enqueued multiple times.
    function enqueue() {
        for (const src in state) { // eslint-disable-line no-restricted-syntax
            if (state[src].stage === STAGE.ready) {
                state[src].stage = STAGE.queue;
                dirty = true;
                return src;
            }
        }
        return null;
    }

    // Inform clients how many files are ready to be enqueued for work.
    function numReady() {
        return _.sum(_.map(state, (props) => (props.stage === STAGE.ready ? 1 : 0)));
    }

    // Return decompressed path ready for slicing.
    async function decompress(src) {
        const data = state[src];
        const decompressPath = workPath('decompress', data.name);
        if (data.stage === STAGE.queue) {
            const { code, stderr, stdout } = await _decompress(src, decompressPath);
            if (code > 0) {
                const err = new Error(`failed to decompress: ${src}`);
                err.src = src;
                err.code = code;
                err.stderr = stderr;
                err.stdout = stdout;
                throw err;
            }
            data.stage = STAGE.decompressed;
            dirty = true;
        }
        const slicePath = workPath('slice', data.name);
        if (data.stage === STAGE.decompressed) {
            await fse.rename(decompressPath, slicePath);
            data.stage = STAGE.slice;
            dirty = true;
        }
        return slicePath;
    }

    // Decompress file in a sub-shell to keep event loop free. Exit code,
    // stdout, & stderr return - error handling pushed to the caller.
    async function _decompress(src, dst) {
        const proc = spawn(`${assetDir}/bin/decompress`, [src, dst], { env: _.pick(process.env, ['PATH', 'TEST_EXIT_CODE']) });
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
                // TODO: was using this to test multiple files easier.
                // setTimeout(async () => {
                //     resolve({
                //         code, stdout, stderr, src, dst
                //     });
                // }, _.random(10) * 1000);
            });
        });
    }

    return {
        enqueue,
        numReady,
        uploaded,
        decompress,
        archive,
    };
};
