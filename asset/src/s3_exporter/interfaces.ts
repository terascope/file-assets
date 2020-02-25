import { OpConfig } from '@terascope/job-components';
import { Compression } from '../__lib/compression';
import { Format } from '../__lib/parser';

export interface S3ExportConfig extends OpConfig {
    path: string;
    extension: string;
    user: string;
    connection: string;
    compression: Compression;
    format: Format;
    field_delimiter: string;
    line_delimiter: string;
    fields: string[];
    include_header: boolean;
    file_per_slice: string;
}
