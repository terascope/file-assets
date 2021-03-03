import type json2csv from 'json2csv';
import type { DataEntity } from '@terascope/utils';

/**
 * Used for defining the slice records type definition
*/
export type SendRecord = Record<string, unknown>|DataEntity;
export type SendRecords = SendRecord[];

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

export enum Format {
    json = 'json',
    ldjson = 'ldjson',
    raw = 'raw',
    tsv = 'tsv',
    csv = 'csv',
}

export interface ChunkedFileReaderConfig extends ChunkedAPIMethods {
    format: Format;
    compression?: Compression;
    /**
     * Specifies the ending line format, override for non unix like file endings
     * @default \n
    */
    line_delimiter?: string;
    file_per_slice?: boolean;

    /** Parameter to determine how the default rejectFn works,
     * may be set to "throw", "log", or "none"
     * @default  "throw"
     */
    on_reject_action?: string;
}

export interface ChunkedFileSenderConfig extends ChunkedAPIMethods {
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
    compression?: Compression;
    /**
     * Specifies the ending line format, override for non unix like file endings
     * @default \n
    */
    line_delimiter?: string;
    /** Set this to override the default extension of a file, will default to the
     * modifiers from format and compression */
    extension?: string;
    file_per_slice?: boolean;
    concurrency?: number
}

export interface CSVReaderConfig extends ChunkedFileReaderConfig {
    format: Format.csv|Format.tsv;
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

export function isCSVReaderConfig(config: ChunkedFileReaderConfig): config is CSVReaderConfig {
    return config.format === Format.csv || config.format === Format.tsv;
}

export function getFieldDelimiter(config: CSVReaderConfig): string {
    if (config.format === Format.tsv) return config.field_delimiter ?? '\t';
    return config.field_delimiter ?? ',';
}

export function getLineDelimiter(config: ChunkedFileSenderConfig|ChunkedFileReaderConfig): string {
    return config.line_delimiter ?? '\n';
}

export function getFieldsFromConfig(
    config: ChunkedFileSenderConfig|ChunkedFileReaderConfig
): string[]|undefined {
    const fields = (config as any).fields as string[]|undefined;
    return fields?.length ? fields : undefined;
}

export interface CSVSenderConfig extends ChunkedFileSenderConfig {
    format: Format.csv|Format.tsv;

    /**
     * List of fields to process, will default to all of them
     * @default []
     */
    fields?: string[];

    /** determines whether or not csv/tsv file will contain a title column.
     * @default false
     */
    include_header?: boolean;

    /**
     * delimiter used for separating columns
     * @default ','
     */
    field_delimiter?: string;
}

export function isCSVSenderConfig(config: ChunkedFileReaderConfig): config is CSVSenderConfig {
    return config.format === Format.csv || config.format === Format.tsv;
}

export interface JSONReaderConfig extends ChunkedFileReaderConfig {
    format: Format.json;

    /**
     * List of fields to process, will default to all of them
     * @default []
     */
    fields?: string[];
}

export function isJSONReaderConfig(config: ChunkedFileReaderConfig): config is JSONReaderConfig {
    return config.format === Format.json;
}

export interface JSONSenderConfig extends ChunkedFileSenderConfig {
    format: Format.json;

    /**
     * List of fields to process, will default to all of them
     * @default []
     */
    fields?: string[];
}

export function isJSONSenderConfig(config: ChunkedFileReaderConfig): config is JSONSenderConfig {
    return config.format === Format.json;
}

export interface LDJSONReaderConfig extends ChunkedFileReaderConfig {
    format: Format.ldjson;
    /**
     * List of fields to process, will default to all of them
     * @default []
     */
    fields?: string[];
}

export function isLDJSONReaderConfig(
    config: ChunkedFileReaderConfig
): config is LDJSONReaderConfig {
    return config.format === Format.ldjson;
}

export interface LDJSONSenderConfig extends ChunkedFileSenderConfig {
    format: Format.ldjson;

    /**
     * List of fields to process, will default to all of them
     * @default []
     */
    fields?: string[];
}

export function isLDJSONSenderConfig(
    config: ChunkedFileReaderConfig
): config is LDJSONSenderConfig {
    return config.format === Format.ldjson;
}

export interface ReaderConfig extends ChunkedFileReaderConfig {
    size: number,
    path: string;
}

export interface HDFSReaderConfig extends ChunkedFileReaderConfig {
    user: string;
    size: number
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
    /**
     * The format of the file that will be read
    */
    format: Format;

    /**
     * The number of bytes to read per slice
    */
    size: number;

    /**
     * Determines if a new file is created for each slice.
    */
    file_per_slice?: boolean;

    /**
     * Optionally override the line delimiter
    */
    line_delimiter?: string;
}

export interface FileSliceConfig extends SliceConfig {
    path: string;
}

export type FetcherFn = (slice: FileSlice) => Promise<string>
