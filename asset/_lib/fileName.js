'use strict';

const path = require('path');

function getName(id, count, opConfig, pathOverride) {
    // Can't use path.join() here since the path might include a filename prefix
    let fileName;
    if (pathOverride !== undefined) {
        fileName = path.join(pathOverride, id);
    } else {
        fileName = path.join(opConfig.path, id);
    }
    // The slice count is only added for `file_per_slice`
    if (opConfig.file_per_slice) {
        fileName += `.${count}`;
    }
    if (opConfig.extension) {
        fileName += `${opConfig.extension}`;
    }
    return fileName;
}

// Parses the provided path and translates it to a bucket/prefix combo
function parsePath(objPath) {
    const pathInfo = {
        bucket: '',
        prefix: ''
    };
    // let hasPrefix = false;
    // if (path[path.length - 1] !== '/') hasPrefix = true;
    const splitPath = objPath.split('/');
    if (objPath[0] !== '/') {
        [pathInfo.bucket] = splitPath;
        splitPath.shift();
        if (splitPath.length > 0) {
            // Protects from adding a leading '/' to the object prefix
            if (splitPath[0].length !== 0) {
                // Ensure the prefix ends with a trailing '/'
                pathInfo.prefix = path.join(splitPath.join('/'), '/');
            }
        }
    } else {
        const bucket = splitPath[1];
        pathInfo.bucket = bucket;
        // Remove the empty string and the root dir (bucket)
        splitPath.shift();
        splitPath.shift();
        if (splitPath.length > 0) {
            // Protects from adding a leading '/' to the object prefix
            if (splitPath[0].length !== 0) {
                // Ensure the prefix ends with a trailing '/'
                pathInfo.prefix = path.join(splitPath.join('/'), '/');
            }
        }
    }
    return pathInfo;
}

module.exports = {
    getName,
    parsePath
};
