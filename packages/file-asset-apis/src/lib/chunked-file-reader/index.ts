import {
    Logger,
    cloneDeep,
    DataEntity,
    isNotNil,
    AnyObject
} from '@terascope/job-components';
import csvToJson from 'csvtojson';
import { CSVParseParam } from 'csvtojson/v2/Parameters';
import { SlicedFileResults, ChunkedConfig, Compression } from '../../interfaces';
import { CompressionFormatter } from '../compression';

type FN = (input: any) => any;

export abstract class ChunkedFileReader extends CompressionFormatter {
    config: ChunkedConfig;
    logger: Logger;
    private tryFn: (fn:(msg: any) => DataEntity) => (input: any) => DataEntity | null;
    private rejectRecord: (input: unknown, err: Error) => never | null;

    constructor(config: ChunkedConfig, logger: Logger) {
        super(config.compression);
        this.config = config;
        this.logger = logger;
        this.tryFn = config.tryFn || this.tryCatch;
        this.rejectRecord = config.rejectFn || this.reject;

        // file_per_slice must be set to true if compression is set to anything besides "none"
        if (config.compression !== Compression.none && config.file_per_slice !== true) {
            throw new Error('Invalid parameter "file_per_slice", it must be set to true if compression is set to anything other than "none" as we cannot properly divide up a compressed file');
        }
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
        const action = this.config._dead_letter_action;
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
        slice: SlicedFileResults,
    ): Promise<(DataEntity<any, any>)[]> {
        const delimiter = this.config.line_delimiter;

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

        const results = await this[this.config.format](collectedData, slice);

        if (results) return results.filter(isNotNil) as DataEntity<any, any>[];
        return results;
    }

    protected async getMargin(slice: SlicedFileResults, delimiter: string): Promise<string> {
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
        incomingData: string, slice: SlicedFileResults
    ): Promise<(DataEntity | null)[]> {
        const data = splitChunks(incomingData, this.config.line_delimiter, slice);
        return data.map(
            this.tryFn((record: any) => DataEntity.make({ data: record }, slice))
        );
    }

    protected async csv(
        incomingData: string, slice: SlicedFileResults, runAsTSV = false
    ): Promise<(DataEntity | null)[]> {
        const csvParams = Object.assign({
            delimiter: runAsTSV ? '\t' : this.config.field_delimiter,
            headers: this.config.fields,
            trim: true,
            noheader: true,
            ignoreEmpty: this.config.ignore_empty || false,
            output: 'json'
        } as Partial<CSVParseParam>, this.config.extra_args);

        let foundHeader = false;
        const data = splitChunks(incomingData, this.config.line_delimiter, slice);

        const processChunk = async (record: string) => {
            try {
                const cvsJson = await csvToJson(csvParams).fromString(record);
                let parsedLine = cvsJson[0];

                // csvToJson trim applied inconsistently so implemented this function
                Object.keys(parsedLine).forEach((key) => {
                    parsedLine[key] = parsedLine[key].trim();
                });
                // Check for header row. Assumes there would only be one header row in a slice
                if (this.config.remove_header && !foundHeader
                    && Object.keys(parsedLine)
                        .sort().join() === Object.values(parsedLine).sort().join()) {
                    foundHeader = true;
                    parsedLine = null;
                }
                if (parsedLine) {
                    return DataEntity.fromBuffer(
                        JSON.stringify(parsedLine),
                        this.config,
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
        incomingData: string, slice: SlicedFileResults
    ): Promise<(DataEntity | null)[]> {
        return this.csv(incomingData, slice, true);
    }

    protected async json(
        incomingData: string, slice: SlicedFileResults
    ): Promise<(DataEntity | null)[]> {
        const data = JSON.parse(incomingData);

        if (Array.isArray(data)) {
            return data.map(
                this.tryFn((record: any) => DataEntity.fromBuffer(
                    JSON.stringify(record),
                    this.config,
                    slice
                ))
            );
        }

        try {
            return [DataEntity.fromBuffer(
                JSON.stringify(data),
                this.config,
                slice
            )];
        } catch (err) {
            return [this.rejectRecord(data, err)];
        }
    }

    protected async ldjson(
        incomingData: string, slice: SlicedFileResults
    ): Promise<(DataEntity | null)[]> {
        const data = splitChunks(incomingData, this.config.line_delimiter, slice);

        return data.map(
            this.tryFn((record: any) => DataEntity.fromBuffer(
                record,
                this.config,
                slice
            ))
        );
    }

    async read(slice: SlicedFileResults): Promise<DataEntity[]> {
        return this.getChunk(slice);
    }
}

function _averageRecordSize(array: string[]): number {
    return Math.floor(array.reduce((allChars, str) => allChars + str.length, 0) / array.length);
}

// This function takes the raw data and breaks it into records, getting rid
// of anything preceding the first complete record if the data does not
// start with a complete record.
function splitChunks(rawData: string, delimiter: string, slice: SlicedFileResults) {
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
