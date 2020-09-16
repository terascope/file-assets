import {
    Slicer,
    WorkerContext,
    ExecutionConfig,
    SlicerRecoveryData
} from '@terascope/job-components';
import { FileReaderConfig } from './interfaces';
import { SliceConfig } from '../__lib/interfaces';
import { FileReaderFactoryAPI } from '../file_reader_api/interfaces';
import FileSlicer from '../file_reader_api/file-slicer';

export default class FileSlicerOperation extends Slicer {
    directories: string[];
    _doneSlicing = false;
    sliceConfig: SliceConfig;
    slicer!: FileSlicer;

    constructor(
        context: WorkerContext, opConfig: FileReaderConfig, executionConfig: ExecutionConfig
    ) {
        super(context, opConfig, executionConfig);
        this.directories = [opConfig.path];
        this.sliceConfig = Object.assign({}, opConfig);
    }

    /**
     * Currently only enable autorecover jobs
     *
     * @todo we should probably support full recovery
    */
    isRecoverable(): boolean {
        return Boolean(this.executionConfig.autorecover);
    }

    async initialize(recoveryData: SlicerRecoveryData[]): Promise<void> {
        await super.initialize(recoveryData);

        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<FileReaderFactoryAPI>(apiName);
        const api = await apiManager.create(apiName, {});

        api.validatePath(this.opConfig.path);

        const {
            file_per_slice,
            format,
            size,
            line_delimiter,
            path
        } = this.opConfig;

        const config = {
            file_per_slice,
            format,
            size,
            line_delimiter,
            path
        };

        this.slicer = await api.makeFileSlicer(config);
    }

    async slice(): Promise<any[]|null> {
        return this.slicer.slice();
    }
}
