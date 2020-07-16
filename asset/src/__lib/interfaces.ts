import json2csv from 'json2csv';
import { OpConfig } from '@terascope/job-components';
import { ReaderFileConfig } from './common-schema';

export type CSVOptions = json2csv.Options<any>;

export interface ProcessorConfig extends ReaderFileConfig, OpConfig{}

export enum Format {
    json = 'json',
    ldjson = 'ldjson',
    raw = 'raw',
    tsv = 'tsv',
    csv = 'csv',
}

export enum FileSenderType {
    file = 'file',
    s3 = 's3',
    hdfs = 'hdfs'
}

export interface Offsets {
    length: number;
    offset: number;
}

export interface CSVConfig {
    fields: string[];
    include_header: boolean;
    line_delimiter: string;
    field_delimiter: string;
    format: Format;
}

export interface ParseOptions {
    fields: string[];
    line_delimiter: string;
    format: Format;
    compression: Compression;
}

export enum Compression {
    none = 'none',
    lz4 = 'lz4',
    gzip = 'gzip'
}

export interface NameOptions {
    filePath: string;
    filePerSlice?: boolean;
    extension?: string;
}

export interface SlicedFileResults extends Offsets {
    path: string;
    total: number;
}

export interface SliceConfig {
    file_per_slice: boolean;
    format: Format;
    size: number;
    line_delimiter: string;
}

export type FetcherFn = (slice: SlicedFileResults) => Promise<string>
