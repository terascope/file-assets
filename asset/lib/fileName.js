'use strict';

function getName(id, count, opConfig) {
    // Can't use path.join() here since the path might include a filename prefix
    let fileName = `${opConfig.path}${id}`;
    // The slice count is only added for `file_per_slice`
    if (opConfig.file_per_slice) {
        fileName += `.${count}`;
    }
    if (opConfig.extension) {
        fileName += `.${opConfig.extension}`;
    }
    return fileName;
}

// Parses the provided path and translates it to a bucket/prefix combo
function parsePath(path) {
    const pathInfo = {
        bucket: '',
        prefix: ''
    };
    // let hasPrefix = false;
    // if (path[path.length - 1] !== '/') hasPrefix = true;
    const splitPath = path.split('/');
    if (path[0] !== '/') {
        [pathInfo.bucket] = splitPath;
        splitPath.shift();
        if (splitPath.length > 0) {
            pathInfo.prefix = splitPath.join('/');
        }
    } else {
        const bucket = splitPath[1];
        pathInfo.bucket = bucket;
        // Remove the empty string and the root dir (bucket)
        splitPath.shift();
        splitPath.shift();
        if (splitPath.length > 0) {
            pathInfo.prefix = splitPath.join('/');
        }
    }
    return pathInfo;
}

module.exports = {
    getName,
    parsePath
};
