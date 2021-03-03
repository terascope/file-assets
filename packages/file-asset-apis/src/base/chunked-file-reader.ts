import {
    Logger,
    cloneDeep,
    DataEntity,
    isNotNil,
    AnyObject,
    DataEncoding,
    isSimpleObject,
    isString,
    isBoolean,
} from '@terascope/utils';
import csvToJson from 'csvtojson';
import { CSVParseParam } from 'csvtojson/v2/Parameters';
import {
    FileSlice,
    ChunkedFileReaderConfig,
    Compression,
    Format,
    isCSVReaderConfig,
    CSVReaderConfig,
    getFieldDelimiter,
    getLineDelimiter,
    getFieldsFromConfig,
} from '../interfaces';
import { Compressor } from './Compressor';

type FN = (input: any) => any;

function validateCSVConfig(inputConfig: CSVReaderConfig) {
    const {
        extra_args,
        fields,
        ignore_empty,
        remove_header,
        field_delimiter,
        format
    } = inputConfig;

    if (extra_args != null && !isSimpleObject(extra_args)) {
        throw new Error('Invalid parameter extra_args, it must be an object');
    }

    if (fields != null && !Array.isArray(fields)) {
        throw new Error('Invalid parameter fields, it must be an empty array or an array containing strings');
    }

    if (fields != null && !fields.every(isString)) {
        throw new Error('Invalid parameter fields, it must be an array containing strings');
    }

    if (ignore_empty != null && !isBoolean(ignore_empty)) {
        throw new Error('Invalid parameter ignore_empty, it must be a boolean');
    }

    if (remove_header != null && !isBoolean(remove_header)) {
        throw new Error('Invalid parameter remove_header, it must be a boolean');
    }

    if (field_delimiter != null && !isString(field_delimiter)) {
        throw new Error('Invalid parameter field_delimiter, it must be a string');
    }

    // if field_delimiter is given and format is tsv, it must be set to \t
    if (format === Format.tsv && field_delimiter != null && field_delimiter !== '\t') {
        throw new Error(`Invalid parameter field_delimiter, if format is set to ${Format.tsv} and field_delimiter is provided, it must be set to "\\t"`);
    }

    return inputConfig;
}

const formatValues = Object.values(Format);
export abstract class ChunkedFileReader {
    logger: Logger;
    compressor: Compressor;
    private onRejectAction: string;
    private tryFn: (fn:(msg: any) => DataEntity) => (input: any) => DataEntity | null;
    private rejectRecord: (input: unknown, err: Error) => never | null;
    private config: ChunkedFileReaderConfig
    private encodingConfig: {
        _encoding: DataEncoding
    }
    protected filePerSlice: boolean;

    constructor(inputConfig: ChunkedFileReaderConfig, logger: Logger) {
        const {
            on_reject_action = 'throw',
            rejectFn = this.reject,
            tryFn = this.tryCatch,
            compression = Compression.none,
            file_per_slice = false,
            format,
        } = inputConfig;

        const encoding = format === Format.raw ? DataEncoding.RAW : DataEncoding.JSON;

        if (format == null || !formatValues.includes(format)) {
            throw new Error(`Invalid parameter format, is must be provided and be set to any of these: ${formatValues.join(', ')}`);
        }

        if (isCSVReaderConfig(inputConfig)) {
            validateCSVConfig(inputConfig);
        }

        this.config = { ...inputConfig };
        this.logger = logger;
        this.tryFn = tryFn;
        this.rejectRecord = rejectFn;
        this.onRejectAction = on_reject_action;
        this.encodingConfig = {
            _encoding: encoding
        };

        // file_per_slice must be set to true if compression is set to anything besides "none"
        if (compression !== Compression.none && file_per_slice !== true) {
            throw new Error('Invalid parameter "file_per_slice", it must be set to true if compression is set to anything other than "none" as we cannot properly divide up a compressed file');
        }

        this.compressor = new Compressor(inputConfig.compression);
        this.filePerSlice = file_per_slice;
    }

    get lineDelimiter(): string {
        return getLineDelimiter(this.config);
    }

    get format(): Format {
        return this.config.format;
    }

    private tryCatch(fn: FN) {
        return (input: any) => {
            try {
                return fn(input);
            } catch (err) {
                this.reject(input, err);
            }
        };
    }

    private reject(input: unknown, err: Error): never | null {
        const action = this.onRejectAction;
        if (action === 'throw' || !action) {
            throw err;
        }

        if (action === 'none') return null;

        if (action === 'log') {
            this.logger.error(err, 'Bad record', input);
            return null;
        }

        return null;
    }

    protected abstract fetch(msg: AnyObject): Promise<string>

    // This method will grab the chunk of data specified by the slice plus an
    // extra margin if the slice does not end with the delimiter.
    private async getChunk(
        slice: FileSlice,
    ): Promise<(DataEntity<any, any>)[]> {
        const delimiter = this.lineDelimiter;

        let needMargin = false;
        if (slice.length) {
            // Determines whether or not to grab the extra margin.
            if (slice.offset + slice.length !== slice.total) {
                needMargin = true;
            }
        }

        const data = await this.fetch(slice);
        let collectedData = data;

        if (data.endsWith(delimiter)) {
            // Skip the margin if the raw data ends with the delimiter since
            // it will end with a complete record.
            needMargin = false;
        }

        if (needMargin) {
            // Want to minimize reads since will typically be over the
            // network. Using twice the average record size as a heuristic.
            const avgSize = _averageRecordSize(data.split(delimiter));
            const newSlice = cloneDeep(slice);
            newSlice.offset = slice.offset + slice.length;
            newSlice.length = 2 * avgSize;

            collectedData += await this.getMargin(newSlice, delimiter);
        }

        const results = await this[this.format](collectedData, slice);

        if (results) return results.filter(isNotNil) as DataEntity<any, any>[];
        return results;
    }

    protected async getMargin(slice: FileSlice, delimiter: string): Promise<string> {
        const { offset, length } = slice;
        let margin = '';
        let currentOffset = offset;

        while (margin.indexOf(delimiter) === -1) {
            // reader clients must return false-y when nothing more to read.
            const newSlice = cloneDeep(slice);
            newSlice.offset = currentOffset;

            const chunk = await this.fetch(newSlice);

            if (!chunk) {
                return margin.split(delimiter)[0];
            }

            margin += chunk;
            currentOffset += length;
        }
        // Don't read too far - next slice will get it.
        return margin.split(delimiter)[0];
    }

    // No parsing, leaving to reader or a downstream op.
    protected async raw(
        incomingData: string, slice: FileSlice
    ): Promise<(DataEntity | null)[]> {
        const data = splitChunks(incomingData, this.lineDelimiter, slice);
        return data.map(
            this.tryFn((record: any) => DataEntity.make({ data: record }, slice))
        );
    }

    protected async csv(
        incomingData: string, slice: FileSlice
    ): Promise<(DataEntity | null)[]> {
        const config = this.config as CSVReaderConfig;
        const removeHeader = config.remove_header ?? true;
        const ignoreEmpty = config.ignore_empty ?? true;

        const csvParams = Object.assign({
            delimiter: getFieldDelimiter(config),
            headers: getFieldsFromConfig(config) ?? [],
            trim: true,
            noheader: true,
            ignoreEmpty,
            output: 'json'
        } as Partial<CSVParseParam>, config.extra_args);

        let foundHeader = false;
        const data = splitChunks(incomingData, this.lineDelimiter, slice);

        const processChunk = async (record: string) => {
            try {
                const cvsJson = await csvToJson(csvParams).fromString(record);
                let parsedLine = cvsJson[0];

                // csvToJson trim applied inconsistently so implemented this function
                Object.keys(parsedLine).forEach((key) => {
                    parsedLine[key] = parsedLine[key].trim();
                });
                // Check for header row. Assumes there would only be one header row in a slice
                if (removeHeader && !foundHeader
                    && Object.keys(parsedLine)
                        .sort().join() === Object.values(parsedLine).sort().join()) {
                    foundHeader = true;
                    parsedLine = null;
                }
                if (parsedLine) {
                    return DataEntity.fromBuffer(
                        JSON.stringify(parsedLine),
                        this.encodingConfig,
                        slice
                    );
                }
                return null;
            } catch (err) {
                return this.rejectRecord(record, err);
            }
        };

        const actions = data.map(processChunk);
        return Promise.all(actions);
    }

    // tsv is just a specific case of csv
    protected async tsv(
        incomingData: string, slice: FileSlice
    ): Promise<(DataEntity | null)[]> {
        return this.csv(incomingData, slice);
    }

    protected async json(
        incomingData: string, slice: FileSlice
    ): Promise<(DataEntity | null)[]> {
        const data = JSON.parse(incomingData);

        if (Array.isArray(data)) {
            return data.map(
                this.tryFn((record: any) => DataEntity.fromBuffer(
                    JSON.stringify(record),
                    this.encodingConfig,
                    slice
                ))
            );
        }

        try {
            return [DataEntity.fromBuffer(
                JSON.stringify(data),
                this.encodingConfig,
                slice
            )];
        } catch (err) {
            return [this.rejectRecord(data, err)];
        }
    }

    protected async ldjson(
        incomingData: string, slice: FileSlice
    ): Promise<(DataEntity | null)[]> {
        const data = splitChunks(incomingData, this.lineDelimiter, slice);

        return data.map(
            this.tryFn((record: any) => DataEntity.fromBuffer(
                record,
                this.encodingConfig,
                slice
            ))
        );
    }

    async read(slice: FileSlice): Promise<DataEntity[]> {
        return this.getChunk(slice);
    }
}

function _averageRecordSize(array: string[]): number {
    return Math.floor(array.reduce((allChars, str) => allChars + str.length, 0) / array.length);
}

// This function takes the raw data and breaks it into records, getting rid
// of anything preceding the first complete record if the data does not
// start with a complete record.
function splitChunks(rawData: string, delimiter: string, slice: FileSlice) {
    // Since slices with a non-zero chunk offset grab the character
    // immediately preceding the main chunk, if one of those chunks has a
    // delimiter as the first or second character, it means the chunk starts
    // with a complete record. In this case as well as when the chunk begins
    // with a partial record, splitting the chunk into an array by its
    // delimiter will result in a single garbage record at the beginning of
    // the array. If the offset is 0, the array will never start with a
    // garbage record

    let outputData = rawData;
    if (rawData.endsWith(delimiter)) {
        // Get rid of last character if it is the delimiter since that will
        // just result in an empty record.
        outputData = rawData.slice(0, -delimiter.length);
    }

    if (slice.offset === 0) {
        return outputData.split(delimiter);
    }

    return outputData.split(delimiter).slice(1);
}
