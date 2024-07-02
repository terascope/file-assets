import FileBatcher from './file_exporter/processor.js';
import FileExporterSchema from './file_exporter/schema.js';

import FileFetcher from './file_reader/fetcher.js';
import FileSlicerOperation from './file_reader/slicer.js';
import FileReaderSchema from './file_reader/schema.js';

import FileReaderAPI from './file_reader_api/api.js';
import FileReaderAPISchema from './file_reader_api/schema.js';

import FileSenderAPI from './file_sender_api/api.js';
import FileSenderAPISchema from './file_sender_api/schema.js';

import HDFSBatcher from './hdfs_append/processor.js';
import HDFSAppendSchema from './hdfs_append/schema.js';

import HDFSFetcher from './hdfs_reader/fetcher.js';
import HDFSReaderSchema from './hdfs_reader/schema.js';
import HDFSFileSlicer from './hdfs_reader/slicer.js';

import HDFSReaderFactoryAPI from './hdfs_reader_api/api.js';
import HDFSReaderAPISchema from './hdfs_reader_api/schema.js';

import HDFSSenderFactoryAPI from './hdfs_sender_api/api.js';
import HDFSSenderAPISchema from './hdfs_sender_api/schema.js';

import S3Batcher from './s3_exporter/processor.js';
import S3ExporterSchema from './s3_exporter/schema.js';

import S3Fetcher from './s3_reader/fetcher.js';
import S3ReaderSchema from './s3_reader/schema.js';
import S3Slicer from './s3_reader/slicer.js';

import S3ReaderAPI from './s3_reader_api/api.js';
import S3ReaderAPISchema from './s3_reader_api/schema.js';

import S3SenderAPI from './s3_sender_api/api.js';
import S3SenderAPISchema from './s3_sender_api/schema.js';

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
