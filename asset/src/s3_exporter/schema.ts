import { ConvictSchema, ValidatedJobConfig } from '@terascope/job-components';
import { S3ExportConfig } from './interfaces.js';
import { opSchema } from '../__lib/common-schema.js';
import { DEFAULT_API_NAME } from '../s3_sender_api/interfaces.js';

export default class Schema extends ConvictSchema<S3ExportConfig> {
    validateJob(job: ValidatedJobConfig): void {
        let opIndex = 0;

        const opConfig = job.operations.find((op, ind) => {
            if (op._op === 's3_exporter') {
                opIndex = ind;
                return op;
            }
            return false;
        });

        if (opConfig == null) throw new Error('Could not find s3_exporter operation in jobConfig');

        const {
            api_name, ...newConfig
        } = opConfig;

        const apiName = api_name || `${DEFAULT_API_NAME}:${opConfig._op}-${opIndex}`;

        // we set the new apiName back on the opConfig so it can reference the unique name
        opConfig.api_name = apiName;

        this.ensureAPIFromConfig(apiName, job, newConfig);
    }

    build(): Record<string, any> {
        return opSchema;
    }
}
