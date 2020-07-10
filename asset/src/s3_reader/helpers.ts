import path from 'path';

// Parses the provided path and translates it to a bucket/prefix combo
export function parsePath(objPath: string): {
    bucket: string;
    prefix: string;
} {
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
                pathInfo.prefix = path.join(...splitPath, '/');
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
                pathInfo.prefix = path.join(...splitPath, '/');
            }
        }
    }

    return pathInfo;
}
