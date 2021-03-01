import type json2csv from 'json2csv';
import type { DataEntity } from '@terascope/utils';

export interface S3PutConfig {
    Bucket: string;
    Key: string;
    Body: Buffer;
}

export type CSVOptions = json2csv.Options<any>;

export interface ChunkedAPIMethods {
    /** can pass in your own custom try/catch logic or use the default */
    tryFn?: (fn:(msg: any) => DataEntity) => (input: any) => DataEntity | null;
    /** Can pass in your own custom error handler, if you do so it
     * will ignore the "on_reject_action" configuration which only works for the default
     * error handler
     */
    rejectFn?: (input: unknown, err: Error) => never | null;
}

interface BaseFileReaderConfig extends ChunkedAPIMethods {
    compression?: Compression;
    line_delimiter?: string;
    format: Format;
    file_per_slice?: boolean
    /** Parameter to determine how the default rejectFn works,
     * may be set to "throw", "log", or "none"
     * @default  "throw"
     */
    on_reject_action?: string;
}

export interface CSVReaderParams {
    extra_args?: CSVOptions;
    /** Ignore the empty value in tsv/csv columns.
     * @default true
     */
    ignore_empty?: boolean;
    /** Determines if header row should be excluded from tsv/csv files
     * @default true
     */
    remove_header?: boolean;
    /**
     * delimiter used for separating columns
     * @default ','
     */
    field_delimiter?: string;
    /**
     * An array of field names to specify the headers of tsv/csv data
     *
     * @default []
     */
    fields?: string[];
}

export interface CSVSenderConfig {
    /**
     * List of fields to process, will default to all of them
     * @default []
     */
    fields: string[];
    /** determines whether or not csv/tsv file will contain a title column.
     * @default false
     */
    include_header: boolean;
    line_delimiter: string;
    /**
     * delimiter used for separating columns
     * @default ','
     */
    field_delimiter?: string;
    format: Format;
}

export interface ChunkedFileReaderConfig extends BaseFileReaderConfig, CSVReaderParams{}

export interface ReaderConfig extends BaseFileReaderConfig, CSVReaderParams {
    size: number,
    path: string;
}

export interface BaseSenderConfig extends Partial<CSVSenderConfig> {
    /** A unique value that is used to help create the filename
     * to prevent clobbering from other senders
    */
    id: string;
    /** Indicator to allow sending to multiple files, used in conjunction with
     * data-entities with the 'standard:route' metadata property */
    dynamic_routing?: boolean;
    /** this is deprecated, only here for backwards compatibility,
     * please use dynamic_routing instead */
    _key?: string;
    path: string;
    format: Format;
    compression?: Compression,
    /** Set this to override the default extension of a file, will default to the
     * modifiers from format and compression */
    extension?: string;
    file_per_slice?: boolean;
    concurrency?: number
}

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

export enum Compression {
    none = 'none',
    lz4 = 'lz4',
    gzip = 'gzip'
}

export interface NameOptions {
    filePerSlice?: boolean;
    /** Should be provided if filePerSlice is set to true */
    sliceCount?: number;
    id: string;
    extension?: string;
    format: Format,
    compression?: Compression
}

export interface Offsets {
    /**
     * The amount of bytes being read in the slice
     */
    length: number;
    offset: number;
}

/**
 * The File Slice
*/
export interface FileSlice extends Offsets {
    path: string;
    /**
     * How many bytes are in the file
     */
    total: number;
}

export interface SliceConfig {
    file_per_slice: boolean;
    format: Format;
    size: number;
    line_delimiter: string;
}

export interface FileSliceConfig extends SliceConfig {
    path: string;
}

export type FetcherFn = (slice: FileSlice) => Promise<string>

export interface HDFSReaderConfig extends BaseFileReaderConfig {
    user: string;
    size: number
}
