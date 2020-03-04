import path from 'path';

export interface NameOptions {
    filePath: string;
    filePerSlice?: boolean;
    extension?: string;
}

export function getName(id: string, count: number, config: NameOptions, pathOverride?: string) {
    // Can't use path.join() here since the path might include a filename prefix
    const { filePath, filePerSlice = false, extension } = config;

    let fileName;

    if (pathOverride !== undefined) {
        fileName = path.join(pathOverride, id);
    } else {
        fileName = path.join(filePath, id);
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

// Parses the provided path and translates it to a bucket/prefix combo
export function parsePath(objPath: string) {
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
