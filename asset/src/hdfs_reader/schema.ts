import { ConvictSchema } from '@terascope/job-components';
import { HDFSReaderConfig } from './interfaces';
import { fileReaderSchema } from '../__lib/common-schema';

export default class Schema extends ConvictSchema<HDFSReaderConfig> {
    build() {
        const hdfsSchema = {
            user: {
                doc: 'User to use when reading the files. Default: "hdfs"',
                default: 'hdfs',
                format: 'optional_String'
            },
        };

        return Object.assign({}, fileReaderSchema, hdfsSchema);
    }
}
