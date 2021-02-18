import { ConvictSchema, ValidatedJobConfig } from '@terascope/job-components';
import { HDFSReaderOpConfig } from './interfaces';
import { opSchema } from '../__lib/common-schema';
import { DEFAULT_API_NAME } from '../hdfs_reader_api/interfaces';

export default class Schema extends ConvictSchema<HDFSReaderOpConfig> {
    validateJob(job: ValidatedJobConfig): void {
        let opIndex = 0;

        const opConfig = job.operations.find((op, ind) => {
            if (op._op === 'hdfs_reader') {
                opIndex = ind;
                return op;
            }
            return false;
        });

        if (opConfig == null) throw new Error('Could not find hdfs_reader operation in jobConfig');

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
