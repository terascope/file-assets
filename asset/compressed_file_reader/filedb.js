'use strict';

const fse = require('fs-extra');

module.exports = (dbpath) => {
    let dirty = false;
    let state = {};

    fse.exists(dbpath).then((exists) => {
        if (exists) {
            fse.readJson(dbpath).then((results) => {
                state = results;
            });
        }
    });
    // async function read() {
    //     if (!await fse.exists(dbpath)) {
    //         return {};
    //     }
    //     return fse.readJson(dbpath);
    // }
    // const state = read();

    function write() {
        if (dirty) {
            fse.writeJson(dbpath, state, { spaces: 2 }).then((results) => {
                console.warn('FILEDB YEEE', results);
                dirty = false;
            });
        }
    }
    // TODO: Need to clear this interval on shutdown?
    setInterval(write, 100);

    function add(name) {
        if (state[name]) {
            throw new Error(`file already added: ${name}`);
        }
        state[name] = {
            stage: 'uploaded',
        };
        dirty = true;
    }

    function remove(name) {
        // TODO: Quietly ignore when does not exist?
        delete state[name];
        dirty = true;
    }

    // True if not already picked up for processing.
    function isNew(name) {
        return !state[name] || state[name].stage === 'uploaded';
    }

    // Mark file as successfully decompressed.
    function decompressed(name) {
        state[name].stage = 'decompressed';
        dirty = true;
    }

    // Mark file as successfully moved to ready space.
    function readied(name) {
        state[name].stage = 'readied';
        dirty = true;
    }

    return {
        state,
        dbpath,
        add,
        remove,
        write,
        isNew,
        decompressed,
        readied,
    };
};
