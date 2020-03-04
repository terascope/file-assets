"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const path_1 = __importDefault(require("path"));
function getName(id, count, config, pathOverride) {
    // Can't use path.join() here since the path might include a filename prefix
    const { filePath, filePerSlice = false, extension } = config;
    let fileName;
    if (pathOverride !== undefined) {
        fileName = path_1.default.join(pathOverride, id);
    }
    else {
        fileName = path_1.default.join(filePath, id);
    }
    // The slice count is only added for `file_per_slice`
    if (filePerSlice) {
        fileName += `.${count}`;
    }
    if (extension) {
        fileName += `${extension}`;
    }
    return fileName;
}
exports.getName = getName;
// Parses the provided path and translates it to a bucket/prefix combo
function parsePath(objPath) {
    const pathInfo = {
        bucket: '',
        prefix: ''
    };
    const splitPath = objPath.split('/');
    if (objPath[0] !== '/') {
        [pathInfo.bucket] = splitPath;
        splitPath.shift();
        if (splitPath.length > 0) {
            // Protects from adding a leading '/' to the object prefix
            if (splitPath[0].length !== 0) {
                // Ensure the prefix ends with a trailing '/'
                pathInfo.prefix = path_1.default.join(...splitPath, '/');
            }
        }
    }
    else {
        const bucket = splitPath[1];
        pathInfo.bucket = bucket;
        // Remove the empty string and the root dir (bucket)
        splitPath.shift();
        splitPath.shift();
        if (splitPath.length > 0) {
            // Protects from adding a leading '/' to the object prefix
            if (splitPath[0].length !== 0) {
                // Ensure the prefix ends with a trailing '/'
                pathInfo.prefix = path_1.default.join(...splitPath, '/');
            }
        }
    }
    return pathInfo;
}
exports.parsePath = parsePath;
//# sourceMappingURL=fileName.js.map