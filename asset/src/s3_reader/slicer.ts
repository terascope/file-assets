import { Slicer, SlicerRecoveryData } from '@terascope/job-components';
import { FileSlice } from '@terascope/file-asset-apis';
import { S3ReaderConfig } from './interfaces.js';
import { S3ReaderFactoryAPI } from '../s3_reader_api/interfaces.js';

export default class S3Slicer extends Slicer<S3ReaderConfig> {
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

        const apiName = this.opConfig.api_name as string;
        const apiManager = this.getAPI<S3ReaderFactoryAPI>(apiName);
        const api = await apiManager.create(apiName, {});
        this.slicer = await api.makeSlicer();
    }

    async slice(): Promise<FileSlice[] | null> {
        return this.slicer();
    }
}
