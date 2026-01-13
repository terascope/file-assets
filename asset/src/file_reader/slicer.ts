import { Slicer, SlicerRecoveryData } from '@terascope/job-components';
import { FileSlice } from '@terascope/file-asset-apis';
import { FileReaderFactoryAPI, FileReaderAPIConfig } from '../file_reader_api/interfaces.js';
import { FileReaderConfig } from './interfaces.js';

export default class FileSlicerOperation extends Slicer<FileReaderConfig> {
    slicer!: () => Promise<FileSlice[] | null>;

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

        const apiName = this.opConfig._api_name;
        const apiManager = this.getAPI<FileReaderFactoryAPI>(apiName);
        const api = await apiManager.create(apiName, {});
        const apiConfig = apiManager.getConfig(apiName) as FileReaderAPIConfig;

        api.validatePath(apiConfig.path);

        this.slicer = await api.makeSlicer();
    }

    async slice(): Promise<FileSlice[] | null> {
        return this.slicer();
    }
}
