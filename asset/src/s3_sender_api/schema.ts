import { ConvictSchema, cloneDeep, ValidatedJobConfig } from '@terascope/job-components';
import { S3ExporterAPIConfig, DEFAULT_API_NAME } from './interfaces';
import { fileReaderSchema } from '../__lib/common-schema';

const apiSchema = cloneDeep(fileReaderSchema);
// S3 Objects cannot be appended so it must be a new object each slice
apiSchema.file_per_slice.default = true;

export default class Schema extends ConvictSchema<S3ExporterAPIConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const apiConfigs = job.apis.filter((config) => {
            const apiName = config._name;
            return apiName === DEFAULT_API_NAME || apiName.startsWith(`${DEFAULT_API_NAME}:`);
        });

        apiConfigs.forEach((config) => {
            if (config.file_per_slice == null || config.file_per_slice === false) {
                throw new Error('Invalid parameter "file_per_slice", it must be set to true, cannot be append data to S3 objects');
            }
        });
    }

    build(): Record<string, any> {
        return fileReaderSchema;
    }
}
