import { AnyObject, isPlainObject } from '@terascope/utils';

export function isObject(input: unknown): input is AnyObject {
    return isPlainObject(input);
}
