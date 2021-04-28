import FileBatcher from '../src/file_exporter/processor';
import FileExporterSchema from '../src/file_exporter/schema';

import FileFetcher from '../src/file_reader/fetcher';
import FileSlicerOperation from '../src/file_reader/slicer';
import FileReaderSchema from '../src/file_reader/schema';

import FileReaderAPI from '../src/file_reader_api/api';
import FileReaderAPISchema from '../src/file_reader_api/schema';

import FileSenderAPI from '../src/file_sender_api/api';
import FileSenderAPISchema from '../src/file_sender_api/schema';

import HDFSBatcher from '../src/hdfs_append/processor';
import HDFSAppendSchema from '../src/hdfs_append/schema';

import HDFSFetcher from '../src/hdfs_reader/fetcher';
import HDFSReaderSchema from '../src/hdfs_reader/schema';
import HDFSFileSlicer from '../src/hdfs_reader/slicer';

import HDFSReaderFactoryAPI from '../src/hdfs_reader_api/api';
import HDFSReaderAPISchema from '../src/hdfs_reader_api/schema';

import HDFSSenderFactoryAPI from '../src/hdfs_sender_api/api';
import HDFSSenderAPISchema from '../src/hdfs_sender_api/schema';

import S3Batcher from '../src/s3_exporter/processor';
import S3ExporterSchema from '../src/s3_exporter/schema';

import S3Fetcher from '../src/s3_reader/fetcher';
import S3ReaderSchema from '../src/s3_reader/schema';
import S3Slicer from '../src/s3_reader/slicer';

import S3ReaderAPI from '../src/s3_reader_api/api';
import S3ReaderAPISchema from '../src/s3_reader_api/schema';

import S3SenderAPI from '../src/s3_sender_api/api';
import S3SenderAPISchema from '../src/s3_sender_api/schema';

export const ASSETS = {
    file_exporter: {
        Processor: FileBatcher,
        Schema: FileExporterSchema
    },
    file_reader: {
        Fetcher: FileFetcher,
        Schema: FileReaderSchema,
        Slicer: FileSlicerOperation
    },
    file_reader_api: {
        API: FileReaderAPI,
        Schema: FileReaderAPISchema
    },
    file_sender_api: {
        API: FileSenderAPI,
        Schema: FileSenderAPISchema
    },
    hdfs_append: {
        Processor: HDFSBatcher,
        Schema: HDFSAppendSchema
    },
    hdfs_reader: {
        Fetcher: HDFSFetcher,
        Schema: HDFSReaderSchema,
        Slicer: HDFSFileSlicer
    },
    hdfs_reader_api: {
        API: HDFSReaderFactoryAPI,
        Schema: HDFSReaderAPISchema
    },
    hdfs_sender_api: {
        API: HDFSSenderFactoryAPI,
        Schema: HDFSSenderAPISchema
    },
    s3_exporter: {
        Processor: S3Batcher,
        Schema: S3ExporterSchema,
    },
    s3_reader: {
        Fetcher: S3Fetcher,
        Schema: S3ReaderSchema,
        Slicer: S3Slicer
    },
    s3_reader_api: {
        API: S3ReaderAPI,
        Schema: S3ReaderAPISchema
    },
    s3_sender_api: {
        API: S3SenderAPI,
        Schema: S3SenderAPISchema
    }

};
