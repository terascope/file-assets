import { OpConfig } from '@terascope/job-components';
import { Compression } from '../__lib/compression';
import { Format } from '../__lib/parser';

export interface HDFSConfig extends OpConfig {
    path: string;
    extension: string;
    connection: string;
    compression: Compression;
    format: Format;
    field_delimiter: string;
    line_delimiter: string;
    fields: string[];
    include_header: boolean;
    file_per_slice: boolean;
}
