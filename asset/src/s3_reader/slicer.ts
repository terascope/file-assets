import { Slicer, SlicerRecoveryData } from '@terascope/job-components';
import { S3Slicer as S3SlicerAPI } from '@terascope/file-asset-apis';
import { S3ReaderConfig } from './interfaces';
import { S3ReaderFactoryAPI, S3ReaderAPIConfig } from '../s3_reader_api/interfaces';

export default class S3Slicer extends Slicer<S3ReaderConfig> {
    slicer!: S3SlicerAPI
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
        const apiManager = this.getAPI<S3ReaderFactoryAPI>(apiName);
        // FIXME: remove as any
        const api = await apiManager.create(apiName, {} as any);
        const apiConfig = apiManager.getConfig(apiName) as S3ReaderAPIConfig;
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

        if (apiConfig.compression !== 'none') config.file_per_slice = true;

        this.slicer = await api.makeSlicer(config);
    }

    async slice(): Promise<any|null> {
        return this.slicer.slice();
    }
}
