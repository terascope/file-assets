import { DataEntity } from '@terascope/job-components';
import path from 'path';
import { SliceConfig, SlicedFileResults, Offsets } from './interfaces';

export function sliceFile(file: {
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
