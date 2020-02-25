import { OpConfig } from '@terascope/job-components';
import { Compression } from '../__lib/compression';
import { Format, CsvOptions } from '../__lib/parser';

export interface HDFSReaderConfig extends OpConfig {
    path: string;
    extension: string;
    user: string;
    size: number;
    connection: string;
    compression: Compression;
    format: Format;
    field_delimiter: string;
    line_delimiter: string;
    fields: string[];
    remove_header: boolean;
    ignore_empty: boolean;
    file_per_slice: boolean;
    extra_args: CsvOptions;
}
