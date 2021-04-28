import FileBatcher from '../src/file_exporter/processor';
import Schema from '../src/file_exporter/schema';

import FileFetcher from '../src/file_reader/fetcher';
import FileSlicerOperation from '../src/file_reader/slicer';
import Schema from '../src/file_reader/schema';

export const ASSETS = {
    file_exporter: {
        Processor: FileBatcher,
        Schema
    },
    file_reader: {
        Fetcher: FileFetcher,
        Slicer: FileSlicerOperation,
        Schema
    }
};
