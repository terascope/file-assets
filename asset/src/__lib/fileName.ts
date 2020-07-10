import path from 'path';
import { NameOptions } from './interfaces';

export function getName(
    id: string, count: number, config: NameOptions, pathOverride?: string
): string {
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
