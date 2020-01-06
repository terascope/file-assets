'use strict';

const lz4 = require('lz4');
const { gzip } = require('node-gzip');


async function compress(compression, data) {
    switch (compression) {
    case 'lz4':
        return lz4.encode(data);
    case 'gzip':
        return gzip(data);
    case 'none':
        return data;
    default:
        // This shouldn't happen since the config schemas will protect against it
        throw new Error('Unsupported compression:', compression);
    }
}

module.exports = {
    compress
};
