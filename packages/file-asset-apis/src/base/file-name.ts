import {
    isNumber, isString, isNil, isNotNil, isBoolean
} from '@terascope/utils';
import * as nodePathModule from 'path';
import { NameOptions, Format, Compression } from '../interfaces';

const formatValues = Object.values(Format);
const compressionValues = Object.values(Compression);

function validateOptions(nameOptions: NameOptions) {
    const {
        filePath,
        id,
        filePerSlice,
        sliceCount,
        extension,
        format,
        compression
    } = nameOptions;
    if (isNil(filePath) || !isString(filePath)) {
        throw new Error('Invalid parameter filePath, it must be a string value');
    }

    if (isNil(id) || !isString(id)) {
        throw new Error('Invalid parameter id, it must be a string value');
    }

    if (isNotNil(filePerSlice) && !isBoolean(filePerSlice)) {
        throw new Error('Invalid parameter filePerSlice, it must be a boolean value');
    }

    if (filePerSlice && (sliceCount == null || !isNumber(sliceCount))) {
        throw new Error('Invalid parameter sliceCount, it must be provided when filePerSlice is set to true, and must be a number');
    }

    if (isNotNil(extension) && !isString(extension)) {
        throw new Error('Invalid parameter extension, it must be a string value');
    }

    if (isNotNil(format) && !formatValues.includes(format)) {
        throw new Error('Invalid parameter format, it must be of type Format');
    }

    if (isNotNil(compression) && !compressionValues.includes(compression as any)) {
        throw new Error('Invalid parameter format, it must be of type Compression');
    }
}

export function createFileName(nameOptions: NameOptions): string {
    validateOptions(nameOptions);
    const {
        filePath,
        id,
        filePerSlice = false,
        sliceCount,
        extension,
        format,
        compression = Compression.none
    } = nameOptions;

    let fileName = nodePathModule.join(filePath, id);

    // The slice count is only added for `file_per_slice`
    if (filePerSlice) {
        if (sliceCount == null || !isNumber(sliceCount)) throw new Error('Invalid parameter sliceCount, it must be provided when filePerSlice is set to true, and must be a number');
        fileName += `.${sliceCount}`;
    }

    if (extension && isString(extension)) {
        const override = extension.startsWith('.') ? `${extension}` : `.${extension}`;
        return override;
    }

    let newExtension = '';

    // if it is raw, we don't know what extension as it could be anything
    if (format !== Format.raw) {
        newExtension += `.${format}`;
    }

    if (compression === Compression.lz4) {
        newExtension += `.${compression}`;
    } else if (compression === Compression.gzip) {
        newExtension += '.gz';
    }

    fileName += newExtension;

    return fileName;
}
