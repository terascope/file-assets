import fse from 'fs-extra';
import { Logger } from '@terascope/utils';
import { FileSlicer } from './file-slicer';
import {
    FileSlice,
    ReaderConfig,
    FileSliceConfig,
    SliceConfig,
    BaseSenderConfig
} from '../interfaces';
import { segmentFile, canReadFile } from '../base';
import { FileFetcher } from './file-fetcher';
import { FileSender } from './file-sender';

export class FileTerasliceAPI extends FileFetcher {
    readonly segmentFileConfig: SliceConfig
    readonly slicerConfig: FileSliceConfig;

    constructor(config: ReaderConfig, logger: Logger) {
        super(config, logger);
        const { path, size } = config;
        const { lineDelimiter, format, filePerSlice } = this;

        this.segmentFileConfig = {
            line_delimiter: lineDelimiter,
            format,
            size,
            file_per_slice: filePerSlice
        };

        this.slicerConfig = {
            path,
            ...this.segmentFileConfig
        };
    }

    /**
     * Determines if a file name or file path can be processed, it will return false
     * if the name of path contains a segment that starts with "."
     *
    */
    canReadFile(fileName: string): boolean {
        return canReadFile(fileName);
    }

    /**
     * Determines if a directory can be processed, will throw if it is a symlink
     * or if the directory is empty
     *
     * @example
     *
     *   fileReader.validatePath('some/emptyDir') => Error
     *   fileReader.validatePath('some/symlinkedDir')  => Error
     *   fileReader.validatePath('some/dir') => void
    */
    validatePath(path: string): void {
        try {
            const dirStats = fse.lstatSync(path);

            if (dirStats.isSymbolicLink()) {
                throw new Error(`Directory '${path}' cannot be a symlink`);
            }

            const dirContents = fse.readdirSync(path);

            if (dirContents.length === 0) {
                throw new Error(`Directory '${path}' must not be empty`);
            }
        } catch (err) {
            throw new Error(`Path "${path}" is not valid`);
        }
    }

    /**
     * Create file segments from input, these will can be used to fetch specific chunks of a file
     *
     * @example
     *   const config = {
     *      size: 1000,
     *      file_per_slice: false,
     *      line_delimiter: '\n',
     *      size: 300,
     *      format: "ldjson"
     *   }
     *   const fileReader = new FileReader(config);
     *   const results = fileReader.segmentFile({
     *      path: 'some/file.txt',
     *      size: 2000
     *   });
     *   results === [
     *      {
     *          offset: 0,
     *          length: 1000,
     *          path: 'some/file.txt',
     *          total: 1000
     *      },
     *      {
     *          offset: 1001,
     *          length: 1000,
     *          path: 'some/file.txt',
     *          total: 1000
     *      },
     *   ]
    */
    segmentFile(file: {
        path: string;
        size: number;
    }): FileSlice[] {
        return segmentFile(file, this.segmentFileConfig);
    }

    /**
     * Generates a slicer based off the configs
     *
     * @example
     *   const config = {
     *      size: 1000,
     *      file_per_slice: false,
     *      line_delimiter: '\n',
     *      size: 300,
     *      format: "ldjson"
     *      path: 'some/dir'
     *   }
     *   const fileReader = new FileReader(config);
     *   const slicer = await fileReader.newSlicer();
     *
     *   const results = await slicer.slice();
     *   results === [
     *      {
     *          offset: 0,
     *          length: 1000,
     *          path: 'some/dir/file.txt',
     *          total: 1000
     *      }
     *   ]
    */
    async makeSlicer(): Promise<FileSlicer> {
        return new FileSlicer(this.slicerConfig, this.logger);
    }

    /**
     *
     * Constructs a file sender api
     */
    async makeSender(senderConfig: BaseSenderConfig): Promise<FileSender> {
        const config = Object.assign({}, this.slicerConfig, senderConfig);
        return new FileSender(config, this.logger);
    }
}
