import { isNil } from '@terascope/core-utils';
import { ConvictSchema, ValidatedJobConfig } from '@terascope/job-components';
import { S3ReaderConfig } from './interfaces.js';
import { opSchema } from '../__lib/common-schema.js';

export default class Schema extends ConvictSchema<S3ReaderConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const opConfig = job.operations.find((op) => {
            if (op._op === 's3_reader') {
                return op;
            }
            return false;
        });

        if (opConfig == null) throw new Error('Could not find s3_reader operation in jobConfig');

        const { _api_name: apiName } = opConfig;
        const s3ReaderAPI = job.apis.find((jobAPI) => jobAPI._name === apiName);

        if (isNil(s3ReaderAPI)) throw new Error(`Could not find api: ${apiName} listed on the job`);
    }

    build(): Record<string, any> {
        return opSchema;
    }
}
