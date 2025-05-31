import {
    DataEntity, pMap, isString, Logger
} from '@terascope/utils';
import * as nodePathModule from 'node:path';
import { Compressor } from './Compressor.js';
import { Formatter } from './Formatter.js';
import { createFileName } from './createFileName.js';
import { ChunkGenerator } from './ChunkGenerator.js';
import {
    NameOptions,
    FileSenderType,
    Format,
    Compression,
    ChunkedFileSenderConfig,
    getLineDelimiter,
    SendRecords,
    SendRecord,
} from '../interfaces.js';

const formatValues = Object.values(Format);

/** The arguments for sendToFileDestination */
export interface SendBatchConfig {
    /** The original filename  */
    readonly filename: string;
    /** Async Iterator that provides chunks of data to write  */
    readonly chunkGenerator: ChunkGenerator;
    /**
     * The number of records to send,
     * if this is set to -1 this count is unknown due to
     * it being stored in an iterator
    */
    readonly count: number;
    readonly concurrency?: number;
}

export abstract class ChunkedFileSender {
    protected sliceCount = -1;
    private compressor: Compressor;
    protected formatter: Formatter;
    readonly pathList = new Map<string, boolean>();
    readonly type: FileSenderType;
    readonly config: ChunkedFileSenderConfig;
    readonly logger: Logger;

    constructor(type: FileSenderType, config: ChunkedFileSenderConfig, logger: Logger) {
        if (!formatValues.includes(config.format)) {
            throw new Error(`Invalid parameter format, is must be provided and be set to any of these: ${formatValues.join(', ')}`);
        }

        if (!isString(config.path)) {
            throw new Error('Invalid parameter path, it must be provided and be of type string');
        }

        if (!isString(config.id)) {
            throw new Error('Invalid parameter id, it must be set to a unique string value');
        }

        // Enforce `file_per_slice` for JSON format or compressed output
        if (config.format === Format.json && config.file_per_slice !== true) {
            throw new Error('Invalid parameter "file_per_slice", it must be set to true if format is set to json');
        }

        // file_per_slice must be set to true if compression is set to anything besides "none"
        if (config.compression != null
            && config.compression !== Compression.none
            && config.file_per_slice !== true) {
            throw new Error('Invalid parameter "file_per_slice", it must be set to true if compression is set to anything other than "none" as we cannot properly divide up a compressed file');
        }

        this.type = type;
        this.config = { ...config };
        this.logger = logger;
        this.compressor = new Compressor(config.compression);
        this.formatter = new Formatter(config);
    }

    get id(): string {
        return this.config.id;
    }

    get path(): string {
        return this.config.path;
    }

    get isRouter(): boolean {
        return this.config.dynamic_routing ?? false;
    }

    get concurrency(): number {
        return this.config.concurrency ?? 10;
    }

    get lineDelimiter(): string {
        return getLineDelimiter(this.config);
    }

    get filePerSlice(): boolean {
        return this.config.file_per_slice ?? false;
    }

    get format(): Format {
        return this.config.format;
    }

    get nameOptions(): NameOptions {
        return {
            filePerSlice: this.filePerSlice,
            format: this.format,
            compression: this.config.compression ?? Compression.none,
            extension: this.config.extension,
            id: this.config.id,
        };
    }

    abstract verify(path: string): Promise<void>;

    /**
     * Verifies that the base file path exists and that the destination
     * file doesn't already exist
    */
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

    /**
     * Ensures the root destination file path exists and returns
     * the full destination file path.
     *
     * @note this API might change since it should not have knowledge of the different sender types
    */
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

    /**
     *  Method to help create proper file paths, mainly used in the abstract "verify" method
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
    async prepareDispatch(
        data: SendRecords
    ): Promise<SendBatchConfig[]> {
        const batches: Record<string, SendRecord[]> = {};
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

        return pMap(
            Object.entries(batches),
            async ([filename, list]) => {
                const destName = await this.createFileDestinationName(filename);
                return {
                    filename,
                    dest: destName,
                    chunkGenerator: new ChunkGenerator(this.formatter, this.compressor, list),
                    count: list.length
                };
            }
        );
    }

    /**
     * Increases the internal sliceCount used to create filenames, this is already executed
     * in the "send" method, do not use unless you are implementing your own send logic
     */
    protected incrementCount(): void {
        this.sliceCount += 1;
    }

    protected abstract sendToDestination(
        config: SendBatchConfig
    ): Promise<void>;

    /**
     * Write data to file, uses parent "sendToDestination" method to determine location
     *
     * @example
     *   s3Sender.send([{ some: 'data' }]) => Promise<void>
     *   s3Sender.send([DataEntity.make({ some: 'data' })]) => Promise<void>
    */
    async send(records: SendRecords): Promise<number> {
        const { concurrency } = this;
        this.incrementCount();

        if (!this.filePerSlice) {
            if (this.sliceCount > 0) this.formatter.csvOptions.header = false;
        }

        const dispatch = await this.prepareDispatch(records);

        let affectedRecords = 0;
        await pMap(
            dispatch,
            async (config) => {
                await this.sendToDestination(config);
                affectedRecords += config.count;
            },
            { concurrency }
        );

        return affectedRecords;
    }

    /**
     * Write data to file, uses parent "sendToDestination" method to determine location.
     * Use this to avoid having to bucket the data into different paths. Normally
     * you don't want this but it can be used in specific case.
     *
     * @example
     *   s3Sender.simpleSend([{ some: 'data' }]) => Promise<void>
     *   s3Sender.simpleSend([DataEntity.make({ some: 'data' })]) => Promise<void>
    */
    async simpleSend(
        records: SendRecords, useExperimentalLDJSON?: boolean, batchSize?: number
    ): Promise<void> {
        const { concurrency } = this;
        this.incrementCount();

        if (!this.filePerSlice) {
            if (this.sliceCount > 0) this.formatter.csvOptions.header = false;
        }

        await this.sendToDestination({
            filename: this.path,
            chunkGenerator: new ChunkGenerator(
                this.formatter,
                this.compressor,
                records,
                undefined,
                useExperimentalLDJSON,
                batchSize
            ),
            count: Array.isArray(records) ? records.length : -1,
            concurrency
        });
    }
}
