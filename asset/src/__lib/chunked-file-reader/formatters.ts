import csvToJson from 'csvtojson';
import { DataEntity, Logger } from '@terascope/utils';

// This function takes the raw data and breaks it into records, getting rid
// of anything preceding the first complete record if the data does not
// start with a complete record.
function _toRecords(rawData: any, delimiter: string, slice: any) {
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
export function raw(incomingData: any, logger: Logger, opConfig: any, metadata: any, slice: any) {
    const data = _toRecords(incomingData, opConfig.line_delimiter, slice);

    return data.map((record: any) => {
        try {
            return DataEntity.make(
                { data: record },
                metadata
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

export function csv(incomingData: any, logger: Logger, opConfig: any, metadata: any, slice: any) {
    const csvParams = Object.assign({
        delimiter: opConfig.field_delimiter,
        headers: opConfig.fields,
        trim: true,
        noheader: true,
        ignoreEmpty: opConfig.ignore_empty || false,
        output: 'json'
    }, opConfig.extra_args);

    let foundHeader = false;
    const data = _toRecords(incomingData, opConfig.line_delimiter, slice);

    async function call(record: any) {
        try {
            let parsedLine = await csvToJson(csvParams)
                .fromString(record).then((parsedData) => parsedData[0]);
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
                    metadata
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

    return Promise.all(data.map(call));
}

// tsv is just a specific case of csv
export function tsv(incomingData: any, logger: Logger, opConfig: any, metadata: any, slice: any) {
    return csv(incomingData, logger, opConfig, metadata, slice);
}

export function json(incomingData: any, logger: Logger, opConfig: any, metadata: any) {
    const data = JSON.parse(incomingData);
    if (Array.isArray(data)) {
        return data.map((record) => {
            try {
                return DataEntity.fromBuffer(
                    JSON.stringify(record),
                    opConfig,
                    metadata
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
            metadata
        )];
    } catch (err) {
        if (opConfig._dead_letter_action === 'log') {
            logger.error(err, 'Bad record:', data);
        } else if (opConfig._dead_letter_action === 'throw') {
            throw err;
        }
        return null;
    }
}

export function ldjson(
    incomingData: any, logger: Logger, opConfig: any, metadata: any, slice: any
) {
    const data = _toRecords(incomingData, opConfig.line_delimiter, slice);
    return data.map((record: any) => {
        try {
            return DataEntity.fromBuffer(
                record,
                opConfig,
                metadata
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
