import { isString } from '@terascope/utils';
import path from 'path';
import {
    SliceConfig, FileSlice, Offsets, Format
} from '../interfaces';

/**
 *  Used to slice up a file based on the configuration provided, this is a
 * higher level API, prefer this over getOffsets as that is a lower level API
 *
 * The returned results can be used directly with any "read" method of a reader API
 *
 * @example
 *   const slice = { path: 'some/path', size: 1000 };
     const config = {
        file_per_slice: false,
        line_delimiter: '\n',
        size: 300,
        format: Format.ldjson
     };

     const results = segmentFile(slice, config);

     // outputs => [
        {
            offset: 0, length: 300, path: 'some/path', total: 1000
        },
            offset: 299, length: 301, path: 'some/path', total: 1000
        },
            offset: 599, length: 301, path: 'some/path', total: 1000
        },
        {
            offset: 899, length: 101, path: 'some/path', total: 1000
        }
     ]
 */

export function segmentFile(file: {
    path: string;
    size: number;
}, config: SliceConfig): FileSlice[] {
    const slices: FileSlice[] = [];
    if (config.format === Format.json || config.file_per_slice) {
        slices.push({
            path: file.path,
            offset: 0,
            total: file.size,
            length: file.size
        });
    } else {
        getOffsets(
            config.size,
            file.size,
            config.line_delimiter
        ).forEach((offset: any) => {
            offset.path = file.path;
            offset.total = file.size;
            slices.push(offset);
        });
    }

    return slices;
}

/**
 *  Used to calculate the offsets of a file, this is a lower level API,
 * prefer to use segmentFile over this as it is a higher level API
 */
export function getOffsets(size: number, total: number, delimiter: string): Offsets[] {
    if (total === 0) {
        return [];
    }

    if (total < size) {
        return [{ length: total, offset: 0 }];
    }

    const fullChunks = Math.floor(total / size);
    const delta = delimiter.length;
    const length = size + delta;
    const chunks = [];

    for (let chunk = 1; chunk < fullChunks; chunk += 1) {
        chunks.push({ length, offset: (chunk * size) - delta });
    }

    // First chunk doesn't need +/- delta.
    chunks.unshift({ offset: 0, length: size });
    // When last chunk is not full chunk size.
    const lastChunk = total % size;

    if (lastChunk > 0) {
        chunks.push({ offset: (fullChunks * size) - delta, length: lastChunk + delta });
    }

    return chunks;
}

/**
     * Determines if a file name or file path can be processed, it will return false
     * if the path includes a segment that starts with a "."
     *
     * @example
     *   canReadFile('file.txt')  => true
     *   canReadFile('some/path/file.txt')  => true
     *   canReadFile('some/.private_path/file.txt')  => false
    */
export function canReadFile(fileName: string): boolean {
    if (!isString(fileName)) return false;

    const args = fileName.split('/');
    const hasDot = args.some((segment) => segment.charAt(0) === '.');

    if (hasDot) return false;
    return true;
}

/**
 * Parses the provided path and translates it to a bucket/prefix combo
 *
 * Primarily a helper for s3 operations
 *  */
export function parsePath(objPath: string): {
    bucket: string;
    prefix: string;
} {
    if (!isString(objPath)) throw new Error('Must provide path and it must be of type string');

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
