import {
    DataEntity, pMap, isString
} from '@terascope/utils';
import * as nodePathModule from 'path';
import { CompressionFormatter } from './compression';
import { FileFormatter } from './file-formatter';
import { createFileName } from './file-name';
import {
    NameOptions,
    FileSenderType,
    Format,
    Compression,
    ChunkedFileSenderConfig,
} from '../interfaces';

const formatValues = Object.values(Format);

export abstract class ChunkedFileSender {
    readonly id: string;
    readonly nameOptions: NameOptions;
    protected sliceCount = -1;
    readonly format: Format;
    readonly isRouter: boolean;
    private compressionFormatter: CompressionFormatter
    protected fileFormatter: FileFormatter
    readonly pathList = new Map<string, boolean>();
    readonly type: FileSenderType;
    readonly concurrency: number;
    readonly filePerSlice: boolean;
    readonly lineDelimiter: string;
    readonly path: string;

    constructor(type: FileSenderType, config: ChunkedFileSenderConfig) {
        const {
            path, id, format, compression = Compression.none,
            file_per_slice = false, dynamic_routing = false,
            line_delimiter = '\n', extension,
            concurrency = 10,
        } = config;

        if (!formatValues.includes(format)) {
            throw new Error(`Invalid paramter format, is must be provided and be set to any of these: ${formatValues.join(', ')}`);
        }

        if (!isString(path)) {
            throw new Error('Invalid parameter path, it must be provided and be of type string');
        }

        if (!isString(id)) {
            throw new Error('Invalid parameter id, it must be set to a unique string value');
        }

        // Enforce `file_per_slice` for JSON format or compressed output
        if (format === Format.json && config.file_per_slice !== true) {
            throw new Error('Invalid parameter "file_per_slice", it must be set to true if format is set to json');
        }

        // file_per_slice must be set to true if compression is set to anything besides "none"
        if (config.compression != null
            && config.compression !== Compression.none
            && config.file_per_slice !== true) {
            throw new Error('Invalid parameter "file_per_slice", it must be set to true if compression is set to anything other than "none" as we cannot properly divide up a compressed file');
        }

        this.type = type;
        this.id = id;
        this.format = format;
        this.path = path;

        this.nameOptions = {
            filePerSlice: file_per_slice,
            format,
            compression,
            extension,
            id
        };

        this.compressionFormatter = new CompressionFormatter(compression);
        this.fileFormatter = new FileFormatter(config);
        this.isRouter = dynamic_routing;
        this.filePerSlice = file_per_slice;
        this.lineDelimiter = line_delimiter;
        this.concurrency = concurrency;
    }

    abstract verify(path: string): Promise<void>

    private async ensurePathing(path: string, removeFilePath = false): Promise<void> {
        if (!this.pathList.has(path)) {
            if (removeFilePath) {
                const route = path.replace(this.path, '');
                // we make sure file_path is not present because its added back in with verify call
                await this.verify(route);
            } else {
                await this.verify(path);
            }
            this.pathList.set(path, true);
        }
    }

    async createFileDestinationName(filePath: string): Promise<string> {
        // Can't use path.join() here since the path might include a filename prefix
        const { nameOptions, path } = this;

        if (this.type === FileSenderType.file || this.type === FileSenderType.hdfs) {
            if (filePath === path) {
                await this.ensurePathing(filePath);
            } else {
                await this.ensurePathing(filePath, true);
            }
        }

        const fileNameConfig = {
            ...nameOptions,
            sliceCount: this.sliceCount
        };

        return createFileName(filePath, fileNameConfig);
    }

    async convertFileChunk(slice: DataEntity[] | null | undefined): Promise<any|null>
    async convertFileChunk(
        slice: Record<string, unknown>[] | null | undefined
    ): Promise<any|null>
    async convertFileChunk(
        slice: (Record<string, unknown> | DataEntity)[] | null | undefined
    ): Promise<any|null> {
        // null or empty slices get an empty output and will get filtered out below
        if (!slice || !slice.length) return null;
        // Build the output string to dump to the object
        const outStr = this.fileFormatter.format(slice);

        // Let the exporters prevent empty slices from making it through
        if (!outStr || outStr.length === 0 || outStr === this.lineDelimiter) {
            return null;
        }

        return this.compressionFormatter.compress(outStr);
    }

    /**
     *  Method to help create proper file paths, mainly used in the abstract "verify" method
     * @param path: string | undefined
     */
    protected joinPath(route?: string): string {
        const { path } = this;
        if (route && route !== path) {
            return nodePathModule.join(path, '/', route);
        }

        return path;
    }
    /**
     * Batches records in a slice into groups based on the "path" config
     * or by the DataEntity metadata 'standard:route' override if
     * dynamic routing is being used, this is called in the "send" method
     *
     */
    prepareDispatch(
        data: DataEntity[]
    ): Record<string, DataEntity[]>
    prepareDispatch(
        data: Record<string, unknown>[]
    ): Record<string, Record<string, unknown>[]>
    prepareDispatch(
        data: (DataEntity | Record<string, unknown>)[]
    ): Record<string, DataEntity | Record<string, unknown>[]> {
        const batches: Record<string, DataEntity | Record<string, unknown>[]> = {};
        const { path } = this;

        batches[path] = [];
        const isDataEntityArray = DataEntity.isDataEntityArray(data);

        for (const record of data) {
            const override = isDataEntityArray ? (record as DataEntity).getMetadata('standard:route') : false;

            if (this.isRouter && override) {
                const routePath = nodePathModule.join(path, '/', override);

                if (!batches[routePath]) {
                    batches[routePath] = [];
                }
                batches[routePath].push(record);
            } else {
                batches[path].push(record);
            }
        }

        return batches;
    }

    /**
     * Creates final filename destination as well as converts and compresses data
     * based on configuration
     */
    async prepareSegment(
        path: string, records: DataEntity[] | null | undefined,
    ): Promise<{ fileName: string, output: Buffer }>
    async prepareSegment(
        path: string, records: Record<string, unknown>[] | null | undefined,
    ): Promise<{ fileName: string, output: Buffer }>
    async prepareSegment(
        path: string, records: (DataEntity |Record<string, unknown>)[] | null | undefined,
    ): Promise<{ fileName: string, output: Buffer }> {
        const fileName = await this.createFileDestinationName(path);
        const output = await this.convertFileChunk(records);
        return { fileName, output };
    }

    /**
     * Increases the internal sliceCount used to create filenames, this is already executed
     * in the "send" method, do not use unless you are implementing your own send logic
     */
    protected incrementCount(): void {
        this.sliceCount += 1;
    }

    protected abstract sendToDestination(
        fileName: string, list: (DataEntity | Record<string, unknown>)[]
    ): Promise<any>

    /**
     * Write data to file, uses parent "sendToDestination" method to determine location
     *
     * @example
     *   s3Sender.send([{ some: 'data' }]) => Promise<void>
     *   s3Sender.send([DataEntity.make({ some: 'data' })]) => Promise<void>
    */
    async send(records: (DataEntity | Record<string, unknown>)[]):Promise<void> {
        const { concurrency } = this;
        this.incrementCount();

        if (!this.filePerSlice) {
            if (this.sliceCount > 0) this.fileFormatter.csvOptions.header = false;
        }

        const dispatch = this.prepareDispatch(records);

        const actions: [string, (DataEntity | Record<string, unknown>)[]][] = [];

        for (const [filename, list] of Object.entries(dispatch)) {
            actions.push([filename, list]);
        }

        await pMap(
            actions,
            ([fileName, list]) => this.sendToDestination(fileName, list),
            { concurrency }
        );
    }
}
