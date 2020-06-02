import csvToJson from 'csvtojson';
import { DataEntity, Logger } from '@terascope/job-components';
import { CSVParseParam } from 'csvtojson/v2/Parameters';
import { SlicedFileResults, ProcessorConfig } from '../interfaces';

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

// No parsing, leaving to reader or a downstream op.
export async function raw(
    incomingData: string, logger: Logger, opConfig: ProcessorConfig, slice: SlicedFileResults
): Promise<(DataEntity | null)[]> {
    const data = splitChunks(incomingData, opConfig.line_delimiter, slice);

    return data.map((record: any) => {
        try {
            return DataEntity.make(
                { data: record },
                slice
            );
        } catch (err) {
            if (opConfig._dead_letter_action === 'log') {
                logger.error(err, 'Bad record:', record);
            } else if (opConfig._dead_letter_action === 'throw') {
                throw err;
            }
            return null;
        }
    });
}

export async function csv(
    incomingData: string, logger: Logger, opConfig: ProcessorConfig, slice: SlicedFileResults
): Promise<(DataEntity | null)[]> {
    const csvParams = Object.assign({
        delimiter: opConfig.field_delimiter,
        headers: opConfig.fields,
        trim: true,
        noheader: true,
        ignoreEmpty: opConfig.ignore_empty || false,
        output: 'json'
    } as Partial<CSVParseParam>, opConfig.extra_args);

    let foundHeader = false;
    const data = splitChunks(incomingData, opConfig.line_delimiter, slice);

    async function processChunk(record: string) {
        try {
            const cvsJson = await csvToJson(csvParams).fromString(record);
            let parsedLine = cvsJson[0];

            // csvToJson trim applied inconsistently so implemented this function
            Object.keys(parsedLine).forEach((key) => {
                parsedLine[key] = parsedLine[key].trim();
            });
            // Check for header row. Assumes there would only be one header row in a slice
            if (opConfig.remove_header && !foundHeader
                && Object.keys(parsedLine)
                    .sort().join() === Object.values(parsedLine).sort().join()) {
                foundHeader = true;
                parsedLine = null;
            }
            if (parsedLine) {
                return DataEntity.fromBuffer(
                    JSON.stringify(parsedLine),
                    opConfig,
                    slice
                );
            }
            return null;
        } catch (err) {
            if (opConfig._dead_letter_action === 'log') {
                logger.error(err, 'Bad record:', record);
            } else if (opConfig._dead_letter_action === 'throw') {
                throw err;
            }
            return null;
        }
    }

    const actions = data.map(processChunk);
    return Promise.all(actions);
}

// tsv is just a specific case of csv
export async function tsv(
    incomingData: string, logger: Logger, opConfig: ProcessorConfig, slice: SlicedFileResults
): Promise<(DataEntity | null)[]> {
    const config = Object.assign({}, opConfig, { field_delimiter: '\t' });
    return csv(incomingData, logger, config, slice);
}

export async function json(
    incomingData: string, logger: Logger, opConfig: ProcessorConfig, slice: SlicedFileResults
): Promise<(DataEntity | null)[]> {
    const data = JSON.parse(incomingData);
    if (Array.isArray(data)) {
        return data.map((record) => {
            try {
                return DataEntity.fromBuffer(
                    JSON.stringify(record),
                    opConfig,
                    slice
                );
            } catch (err) {
                if (opConfig._dead_letter_action === 'log') {
                    logger.error(err, 'Bad record:', record);
                } else if (opConfig._dead_letter_action === 'throw') {
                    throw err;
                }
                return null;
            }
        });
    }
    try {
        return [DataEntity.fromBuffer(
            JSON.stringify(data),
            opConfig,
            slice
        )];
    } catch (err) {
        if (opConfig._dead_letter_action === 'log') {
            logger.error(err, 'Bad record:', data);
        } else if (opConfig._dead_letter_action === 'throw') {
            throw err;
        }
        return [null];
    }
}

export async function ldjson(
    incomingData: string, logger: Logger, opConfig: ProcessorConfig, slice: SlicedFileResults
): Promise<(DataEntity | null)[]> {
    const data = splitChunks(incomingData, opConfig.line_delimiter, slice);
    return data.map((record: any) => {
        try {
            return DataEntity.fromBuffer(
                record,
                opConfig,
                slice
            );
        } catch (err) {
            if (opConfig._dead_letter_action === 'log') {
                logger.error(err, 'Bad record:', record);
            } else if (opConfig._dead_letter_action === 'throw') {
                throw err;
            }
            return null;
        }
    });
}
