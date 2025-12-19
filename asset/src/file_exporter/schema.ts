import { isNil } from '@terascope/core-utils';
import { ConvictSchema, ValidatedJobConfig } from '@terascope/job-components';
import { FileExporterConfig } from './interfaces.js';
import { opSchema } from '../__lib/common-schema.js';

export default class Schema extends ConvictSchema<FileExporterConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const opConfig = job.operations.find((op) => {
            if (op._op === 'file_exporter') {
                return op;
            }
            return false;
        });

        if (opConfig == null) throw new Error('Could not find file_exporter operation in jobConfig');

        const { _api_name: apiName } = opConfig;
        const fileReaderAPI = job.apis.find((jobAPI) => jobAPI._name === apiName);

        if (isNil(fileReaderAPI)) throw new Error(`Could not find api: ${apiName} listed on the job`);
    }

    build(): Record<string, any> {
        return opSchema;
    }
}
