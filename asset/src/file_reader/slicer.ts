import { Slicer, SlicerRecoveryData } from '@terascope/job-components';
import { FileReaderFactoryAPI, FileReaderAPIConfig } from '../file_reader_api/interfaces';
import FileSlicer from '../file_reader_api/file-slicer';

export default class FileSlicerOperation extends Slicer {
    slicer!: FileSlicer;

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
        const apiConfig = apiManager.getConfig(apiName) as FileReaderAPIConfig;

        api.validatePath(apiConfig.path);

        const {
            file_per_slice,
            format,
            size,
            line_delimiter,
            path
        } = apiConfig;

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
