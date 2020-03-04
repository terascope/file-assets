import { DataEntity } from '@terascope/utils';
import { getOffsets, Offsets } from './chunked-file-reader';
import { Format } from './parser';

export interface SlicedFileResults extends Offsets {
    path: string;
    total: string;
}

export interface SliceConfig {
    file_per_slice: boolean;
    format: Format;
    size: number;
    line_delimiter: string;
}

export function sliceFile(file: any, config: SliceConfig): SlicedFileResults[] {
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
export function batchSlice(data: DataEntity[], defaultPath: string) {
    const batches: Record<string, DataEntity[]> = {};
    batches[defaultPath] = [];

    data.forEach((record: any) => {
        const override = record.getMetadata('standard:route');

        if (override) {
            if (!batches[override]) {
                batches[override] = [];
            }
            batches[override].push(record);
        } else {
            batches[defaultPath].push(record);
        }
    });

    return batches;
}
