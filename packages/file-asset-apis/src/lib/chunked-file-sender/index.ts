import {
    isEmpty, DataEntity, AnyObject, pMap
} from '@terascope/job-components';
import * as nodePathModule from 'path';
import { CompressionFormatter } from '../compression';
import { FileFormatter } from '../file-formatter';
import {
    NameOptions,
    FileSenderType,
    Format,
    ChunkedSenderConfig
} from '../../interfaces';

export abstract class ChunkedFileSender {
    readonly workerId: string;
    readonly nameOptions: NameOptions;
    protected sliceCount = -1;
    readonly format: Format;
    readonly isRouter: boolean;
    readonly config: AnyObject;
    private compressionFormatter: CompressionFormatter
    protected fileFormatter: FileFormatter
    readonly pathList = new Map<string, boolean>();
    readonly type: FileSenderType;

    constructor(type: FileSenderType, config: ChunkedSenderConfig) {
        const {
            path, worker_id, format, compression
        } = config;
        this.type = type;
        this.workerId = worker_id;
        this.format = format;
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

        // `filePerSlice` needs to be ignored since you cannot append to S3 objects
        if (type === FileSenderType.s3) {
            this.nameOptions.filePerSlice = true;
        }

        this.compressionFormatter = new CompressionFormatter(compression);
        this.fileFormatter = new FileFormatter(format, config);
        this.config = config;
        this.isRouter = config.dynamic_routing;
    }

    abstract verify(path: string): Promise<void>

    private async ensurePathing(path: string, removeFilePath = false): Promise<void> {
        if (!this.pathList.has(path)) {
            if (removeFilePath) {
                const route = path.replace(this.nameOptions.filePath, '');
                // we make sure file_path is not present because its added back in with verify call
                await this.verify(route);
            } else {
                await this.verify(path);
            }
            this.pathList.set(path, true);
        }
    }

    private async createFileDestinationName(pathing: string): Promise<string> {
        // Can't use path.join() here since the path might include a filename prefix
        const { filePerSlice = false, extension, filePath } = this.nameOptions;
        let fileName: string;

        if (this.type === FileSenderType.file || this.type === FileSenderType.hdfs) {
            if (pathing === filePath) {
                await this.ensurePathing(pathing);
            } else {
                await this.ensurePathing(pathing, true);
            }

            fileName = nodePathModule.join(pathing, this.workerId);
        } else if (this.type === FileSenderType.s3) {
            // we treat this different because of working with a single bucket
            fileName = nodePathModule.join(pathing, this.workerId);
        } else {
            fileName = '';
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

    private async convertFileChunk(slice: DataEntity[] | null | undefined): Promise<any|null>
    private async convertFileChunk(
        slice: Record<string, unknown>[] | null | undefined
    ): Promise<any|null>
    private async convertFileChunk(
        slice: (Record<string, unknown> | DataEntity)[] | null | undefined
    ): Promise<any|null> {
        // null or empty slices get an empty output and will get filtered out below
        if (!slice || !slice.length) return null;
        // Build the output string to dump to the object
        const outStr = this.fileFormatter.format(slice);

        // Let the exporters prevent empty slices from making it through
        if (!outStr || outStr.length === 0 || outStr === this.config.line_delimiter) {
            return null;
        }

        return this.compressionFormatter.compress(outStr);
    }

    /**
     *  Method to help create proper file paths, mainly used in the abstract "verify" method
     * @param path: string | undefined
     */
    protected joinPath(path?: string): string {
        const { filePath } = this.nameOptions;
        if (path && path !== filePath) {
            return nodePathModule.join(filePath, '/', path);
        }
        return filePath;
    }
    /**
     * Batches records in a slice into groups based on the "path" config
     * or by the DataEntity metadata 'standard:route' override if
     * dynamic routing is being used
     *
     */
    protected prepareDispatch(
        data: DataEntity[]
    ): Record<string, DataEntity[]>
    protected prepareDispatch(
        data: Record<string, unknown>[]
    ): Record<string, Record<string, unknown>[]>
    protected prepareDispatch(
        data: (DataEntity | Record<string, unknown>)[]
    ): Record<string, DataEntity | Record<string, unknown>[]> {
        const batches: Record<string, DataEntity | Record<string, unknown>[]> = {};
        const { filePath } = this.nameOptions;

        batches[filePath] = [];
        const isDataEntityArray = DataEntity.isDataEntityArray(data);

        for (const record of data) {
            const override = isDataEntityArray ? (record as DataEntity).getMetadata('standard:route') : false;

            if (this.isRouter && override) {
                const routePath = nodePathModule.join(filePath, '/', override);

                if (!batches[routePath]) {
                    batches[routePath] = [];
                }
                batches[routePath].push(record);
            } else {
                batches[filePath].push(record);
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
    ): Promise<{ fileName: string, output: DataEntity[]| null | undefined | string | Buffer }>
    async prepareSegment(
        path: string, records: Record<string, unknown>[] | null | undefined,
    ): Promise<{ fileName: string, output: DataEntity[]| null | undefined | string | Buffer }>
    async prepareSegment(
        path: string, records: (DataEntity |Record<string, unknown>)[] | null | undefined,
    ): Promise<{ fileName: string, output: DataEntity[]| null | undefined | string | Buffer }> {
        const fileName = await this.createFileDestinationName(path);
        const output = await this.convertFileChunk(records);
        return { fileName, output };
    }

    private incrementCount(): void {
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
        const { concurrency } = this.config;
        this.incrementCount();

        if (!this.config.file_per_slice) {
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
