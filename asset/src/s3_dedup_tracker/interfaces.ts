import { OpConfig } from '@terascope/job-components';

export interface S3DedupTrackerConfig extends OpConfig {
    track_field: string;
    s3_bucket: string;
    report_path: string;
    report_interval: number;
    report_interval_ms: number;
    record_fields: string;
    _connection: string;
}
