import { isNil } from '@terascope/core-utils';
import { ConvictSchema, ValidatedJobConfig } from '@terascope/job-components';
import { S3ExportConfig } from './interfaces.js';
import { opSchema } from '../__lib/common-schema.js';

export default class Schema extends ConvictSchema<S3ExportConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const opConfig = job.operations.find((op) => {
            if (op._op === 's3_exporter') {
                return op;
            }
            return false;
        });

        if (opConfig == null) throw new Error('Could not find s3_exporter operation in jobConfig');

        const { _api_name: apiName } = opConfig;
        const s3SenderAPI = job.apis.find((jobAPI) => jobAPI._name === apiName);

        if (isNil(s3SenderAPI)) throw new Error(`Could not find api: ${apiName} listed on the job`);
    }

    build(): Record<string, any> {
        return opSchema;
    }
}
