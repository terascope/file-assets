import { isPlainObject } from '@terascope/core-utils';

export function isObject(input: unknown): input is Record<string, any> {
    return isPlainObject(input);
}
