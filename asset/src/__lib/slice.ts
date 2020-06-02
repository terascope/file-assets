import { DataEntity } from '@terascope/job-components';
import path from 'path';
import { getOffsets } from './chunked-file-reader';
import { SliceConfig, SlicedFileResults } from './interfaces';

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
