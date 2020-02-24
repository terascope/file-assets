import { OpConfig } from '@terascope/job-components';

export enum Compression {
    none = 'none',
    lz4 = 'lz4',
    gzip = 'gzip'
}

export enum Format {
    json = 'json',
    ldjson = 'ldjson',
    raw = 'raw',
    tsv = 'tsv',
    csv = 'csv',
}

export interface FileExporterConfig extends OpConfig {
    path: string;
    extension: string;
    compression: Compression;
    field_delimiter: string;
    line_delimiter: string;
    fields: string[];
    file_per_slice: boolean;
    include_header: boolean;
    format: Format;
}
