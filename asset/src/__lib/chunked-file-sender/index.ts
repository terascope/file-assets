import {
    isEmpty,
    DataEntity,
    AnyObject,
} from '@terascope/job-components';
import * as nodePathModule from 'path';
import CompressionFormatter from '../compression';
import FileFormatter from '../file-formatter';
import {
    NameOptions,
    FileSenderType,
    CSVOptions,
    CSVConfig,
    Format,
} from '../interfaces';
// TODO: this should live in interfaces
// import { FileConfig } from '../common-schema';

export default class ChunkedSender {
    workerId: string;
    nameOptions: NameOptions;
    sliceCount = -1;
    format: Format;
    readonly config: AnyObject;
    private compressionFormatter: CompressionFormatter
    protected fileFormatter: FileFormatter

    constructor(type: FileSenderType, config: AnyObject) {
        const {
            path, workerId, format, compression
        } = config;

        this.workerId = workerId;
        this.format = format;
        // FIXME: types
        const csvOptions = makeCsvOptions(config as any);
        const extension = isEmpty(config.extension) ? undefined : config.extension;

        this.nameOptions = {
            filePath: path,
            extension,
            filePerSlice: config.file_per_slice
        };

        // Coerce `file_per_slice` for JSON format or compressed output for file type
        if (type === FileSenderType.file && (format === 'json' || compression !== 'none')) {
            this.nameOptions.filePerSlice = true;
        }

        this.compressionFormatter = new CompressionFormatter(compression);
        this.fileFormatter = new FileFormatter(format, config as any, csvOptions);
        this.config = config;
    }

    createFileDestinationName(pathOverride?: string): string {
        // Can't use path.join() here since the path might include a filename prefix
        const { filePath, filePerSlice = false, extension } = this.nameOptions;

        let fileName;

        if (pathOverride !== undefined) {
            fileName = nodePathModule.join(pathOverride, this.workerId);
        } else {
            fileName = nodePathModule.join(filePath, this.workerId);
        }

        // The slice count is only added for `file_per_slice`
        if (filePerSlice) {
            fileName += `.${this.sliceCount}`;
        }

        if (extension) {
            fileName += `${extension}`;
        }

        return fileName;
    }

    private async converFileChunk(slice: DataEntity[] | null | undefined): Promise<any|null> {
    // null or empty slices get an empty output and will get filtered out below
        if (!slice || !slice.length) return null;
        // Build the output string to dump to the object
        // TODO externalize this into a ./lib/ for use with the `file_exporter`
        // let outStr = '';
        const outStr = this.fileFormatter.format(slice);
        // Let the exporters prevent empty slices from making it through
        if (!outStr || outStr.length === 0 || outStr === this.config.line_delimiter) {
            return null;
        }

        return this.compressionFormatter.compress(outStr);
    }

    async prepareSegment(
        slice: DataEntity[] | null | undefined, pathOverride?: string
    ): Promise<{ fileName: string, output: any|null }> {
        const fileName = this.createFileDestinationName(pathOverride);
        const output = await this.converFileChunk(slice);

        return { fileName, output };
    }

    incrementCount(): void {
        this.sliceCount += 1;
    }
}

function makeCsvOptions(config: CSVConfig): CSVOptions {
    const csvOptions: CSVOptions = {};

    if (config.fields.length !== 0) {
        csvOptions.fields = config.fields;
    } else {
        csvOptions.fields = undefined;
    }

    csvOptions.header = config.include_header;
    csvOptions.eol = config.line_delimiter;

    // Assumes a custom delimiter will be used only if the `csv` output format is chosen
    if (config.format === 'csv') {
        csvOptions.delimiter = config.field_delimiter;
    } else if (config.format === 'tsv') {
        csvOptions.delimiter = '\t';
    }

    return csvOptions;
}
