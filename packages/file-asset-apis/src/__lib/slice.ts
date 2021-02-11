import { DataEntity, isString } from '@terascope/job-components';
import path from 'path';
import { SliceConfig, SlicedFileResults, Offsets } from './interfaces';

export function segmentFile(file: {
    path: string;
    size: number;
}, config: SliceConfig): SlicedFileResults[] {
    const slices: SlicedFileResults[] = [];
    if (config.format === 'json' || config.file_per_slice) {
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

// [{offset, length}] of chunks `size` assuming `delimiter` for a file with `total` size.
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

// Batches records in a slice into groups based on the `routingPath` override (if present)
export function batchSlice(data: DataEntity[], defaultPath: string): Record<string, DataEntity[]> {
    const batches: Record<string, DataEntity[]> = {};
    batches[defaultPath] = [];

    data.forEach((record: any) => {
        const override = record.getMetadata('standard:route');

        if (override) {
            const routePath = path.join(defaultPath, '/', override);

            if (!batches[routePath]) {
                batches[routePath] = [];
            }
            batches[routePath].push(record);
        } else {
            batches[defaultPath].push(record);
        }
    });

    return batches;
}

export function canReadFile(fileName: string): boolean {
    if (!isString(fileName)) return false;

    const args = fileName.split('/');
    const hasDot = args.some((segment) => segment.charAt(0) === '.');

    if (hasDot) return false;
    return true;
}

// Parses the provided path and translates it to a bucket/prefix combo
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
