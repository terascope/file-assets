import { BaseSchema } from '@terascope/job-components';
import { S3DedupTrackerConfig } from './interfaces.js';

export default class Schema extends BaseSchema<S3DedupTrackerConfig> {
    build(): Record<string, any> {
        return {
            track_field: {
                doc: 'The field on each incoming record whose value is tracked for uniqueness',
                default: null,
                format: 'required_string'
            },
            s3_bucket: {
                doc: 'S3 bucket where the dedup report will be written',
                default: null,
                format: 'required_string'
            },
            report_path: {
                doc: 'S3 key for the report file (e.g. "reports/dedup-report.json"). The same key is overwritten on each flush.',
                default: null,
                format: 'required_string'
            },
            report_interval: {
                doc: 'Write the report every N slices. Set to 0 to disable count-based flushing (report is still written on job shutdown).',
                default: 100,
                format: (val: unknown): void => {
                    if (typeof val !== 'number' || !Number.isInteger(val) || val < 0) {
                        throw new Error('report_interval must be a non-negative integer');
                    }
                }
            },
            report_interval_ms: {
                doc: 'Write the report if this many milliseconds have elapsed since the last flush. The timer resets whenever a count-based flush occurs. Set to 0 to disable.',
                default: 0,
                format: (val: unknown): void => {
                    if (typeof val !== 'number' || !Number.isInteger(val) || val < 0) {
                        throw new Error('report_interval_ms must be a non-negative integer');
                    }
                }
            },
            record_fields: {
                doc: 'Comma-separated list of record fields to capture as a sample alongside each duplicate entry (e.g. "foo,unique_field"). The values are taken from the first occurrence of each tracked value. Omitted from report when empty.',
                default: '',
                format: 'optional_string'
            },
            _connection: {
                doc: 'The Terafoundation S3 connection to use',
                default: 'default',
                format: 'optional_string'
            }
        };
    }
}
