# s3_reader_api

This is a [teraslice api](https://terascope.github.io/teraslice/docs/jobs/configuration#apis), which encapsulates a specific functionality that can be utilized by any processor, reader or slicer.

The `s3_reader_api` will provide an [api factory](https://terascope.github.io/teraslice/docs/packages/job-components/api/classes/apifactory), which is a singleton that can create, cache and manage multiple file sender apis that can be accessed in any operation through the `getAPI` method on the operation.

## Usage

### Example Processor using a s3 reader api
This is an example of a custom processor using the s3_reader_api.

Example Job

```json
{
    "name" : "testing",
    "workers" : 1,
    "slicers" : 1,
    "lifecycle" : "once",
    "assets" : [
        "file"
    ],
    "apis" : [
        {
            "_name": "s3_reader_api",
            "path": "/app/data/test_files",
            "format": "ldjson",
            "line_delimiter": "\n"
        }
    ],
    "operations" : [
        {
            "_op" : "test-reader"
        },
        {
            "_op" : "some_reader",
            "api_name" : "s3_reader_api"
        }
    ]
}
```

Here is a custom processor for the job described above

```javascript
export default class SomeReader extends Fetcher {
    async initialize() {
        await super.initialize();
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI(apiName);
        this.api = await apiManager.create(apiName);
    }

    async fetch() {
        const slice =  {
            path: '/app/data/test_files/someFile.txt',
            offset: 0,
            total: 364,
            length: 364
        }
        // can do anything with the slice before reading
        return this.api.read(slice);
    }
}
```

## S3 Reader Factory API Methods
### size

this will return how many separate reader apis are in the cache

### get
parameters
- name: String

this will fetch any reader api that is associated with the name provided

### getConfig
parameters
- name: String

this will fetch any reader api config that is associated with the name provided

### create (async)
parameters
- name: String
- configOverrides: Check options below, optional

this will create an instance of a [reader api](#s3_reader_instance), and cache it with the name given. Any
config provided in the second argument will override what is specified in the apiConfig and cache it with the name provided. It will throw an error if you try creating another api with the same name parameter

### remove (async)
parameters
- name: String

this will remove an instance of a reader api from the cache and will follow any cleanup code specified in the api code.

### entries

This will allow you to iterate over the cache name and client of the cache

### keys

This will allow you to iterate over the cache name of the cache

### values

This will allow you to iterate over the clients of the cache

## Example of using the factory methods in a processor
```javascript
// example of api configuration
const apiConfig = {
    _name: 's3_reader_api',
    path: '/app/data/test_files',
    format: 'ldjson',
    line_delimiter: '\n'
}

const apiManager = this.getAPI<ElasticReaderFactoryAPI>(apiName);

apiManager.size() === 0

// this will return an api cached at "normalClient" and it will use the default api config
const normalClient = await apiManager.create('normalClient', {})

apiManager.size() === 1

apiManager.get('normalClient') === normalClient

// this will return an api cached at "overrideClient"
const overrideClient = await apiManager.create('overrideClient', { path: 'other/path', format: 'tsv' })

apiManager.size() === 2

// this will return the full configuration for this client
apiManger.getConfig('overrideClient') === {
    _name: 's3_reader_api',
    path: 'other/path',
    format: 'ldjson',
    line_delimiter: '\n'
}


await apiManger.remove('normalClient');

apiManager.size() === 1

apiManager.get('normalClient') === undefined
```

### S3 Reader Instance
This is the reader class that is returned from the create method of the APIFactory

### fetch
```(slice: SlicedFileResults) => Promise<string>```
parameters:
- slice: {
    path: string,
    total: number (total number of bytes),
    length: number (how many bytes to read),
    offset: number (where to start reading from)
}

This method will send the records to file

```js
// this will read the first 500 bytes of the file
const slice = {
    path: 'some/data/path',
    total: 10000,
    length: 500,
    offset: 0
}
const results = await api.read(docs)
```

### canReadFile
```(filePath: String) => Boolean```
parameters:
- filePath: the path of the file

This is a helper method will return true if the filepath is valid, it will return false if any part of the path or filename starts with a `.`

```js
const badPath1 = 'some/.other/path.txt';
const badPath2 = 'some/other/.path.txt';
const goodPath = 'some/other/path.txt';

api.canReadFile(badPath1) === false;
api.canReadFile(badPath2) === false;
api.canReadFile(goodPath) === true;

```

### segmentFile
```(fileInfo, config: SliceConfig) => SlicedFileResults[]```
parameters:
- fileInfo: {
    path: the path to the file
    size: the size in bytes the file contains
}
- config: {
    file_per_slice: please check [Parameters](#parameters) for more information,
    format: used to determine how the data should be written to file,
    size: how big each slice chunk should be,
    line_delimiter: a delimiter applied between each record or slice
}

This is a helper method what will segment a given file and its byte size into chunks that the reader can process.

```js
const slice = { path: 'some/path', size: 1000 };
const config = {
    file_per_slice: false,
    line_delimiter: '\n',
    size: 300,
    format: Format.ldjson
};

 const results = api.segmentFile(slice, config);

results === [
  {
      offset: 0,
      length: 300,
      path: 'some/path',
      total: 1000
  },
  {
     length: 301,
     offset: 299,
     path: 'some/path',
     total: 1000
  },
  {
      length: 301,
      offset: 599,
      path: 'some/path',
      total: 1000
  },
  {
      offset: 899,
      length: 101,
      path: 'some/path',
      total: 1000
  }
]

```

### makeS3Slicer (async)
```(config: FileSliceConfig) => Promise<S3Slicer>```

This function will generate a slicer which is the s3_reader slicer core component. You can use this to generate slice chunks for your reader.

parameters:
- config: {
    file_per_slice: please check [Parameters](#parameters) for more information,
    format: used to determine how the data should be written to file,
    size: how big each slice chunk should be,
    line_delimiter: a delimiter applied between each record or slice,
    path: the top level directory to search for files
}


```js

const config = {
    file_per_slice: false,
    format: 'ldjson',
    size: 1000,
    line_delimiter: '\n',
    path: 'some/path'
};

const slicer = await api.makeS3Slicer(config);

const slice = await slicer.slice();

slice ===  {
      offset: 0,
      length: 1000,
      path: 'some/path',
      total: 1000
}
```

## Parameters
| Configuration | Description | Type |  Notes |
| --------- | -------- | ------ | ------ |
| \_op| Name of operation, it must reflect the exact name of the file | String | required |
| path | The bucket and optional prefix for data. If there is no `/` in this parameter, it will just be treated as a bucket name, and anything separated from the bucket name with a `/` will be treated as a subdirectory whether or not there is a trailing `/` | String | optional, if path is not provided in the opConfig, it must be provided in the api configuration |
| extension | Optional file extension to add to file names | String | optional, A `.` is not automatically prepended to this value when being added to the filename, if it is desired it must be specified on the extension |
| compression | you may specify a compression algorithm to apply to the data before being written to file, it may be set to `none`, `lz4` or `gzip` | String | optional, defaults `none` |
| fields | a list of all field names present in the file **in the order that they are found**, this essentially acts as the headers. This option is only used for `tsv` and `csv` formats | String[] | optional |
| field_delimiter | A delimiter between field names. This is only used when `format` is set to `csv`  | String | optional, defaults to `,`  |
| line_delimiter | If a line delimiter other than `\n` is used in the files, this option will tell the reader how to read records in the file. This option is ignored for `json` format. See the [format](#format) section for more information how this deliminator is applied for each format. | String | optional, defaults to `\n` |
| file_per_slice | This setting determines if the output for a worker will be in a single file (`false`), or if the worker will create a new file for every slice it processes  (`true`). If set to `true`, an integer, starting at 0, will be appended to the filename and incremented by 1 for each slice a worker processes | Boolean | optional, defaults to `true`. If using `json` format, this option will be overridden to `true` |
| include_header | Determines whether or not to include column headers for the fields in output files. If set to `true`, a header will be added as the first entry to every file created. This option is only used for `tsv` and `csv` formats | Boolean | optional, defaults to `false` |
| format | Used to determine how the data should be written to file, options are: `json`, `ldjson`, `raw`, `csv`, `tsv` | String | optional, defaults to `ldjson`, please reference the [format](#format) section for more information |
| size | Determines the target slice size in bytes. The actual slice size will vary slightly since the reader will read additional bytes from the file in order to complete a record if the read ends with a partial record. This option is ignored for `json` format. See `json` format option below for more info. | Number | optional, defaults to `10000000` |
| remove_header | Checks for the header row in csv or tsv files and removes it | Boolean | optional, defaults to `true` |
| ignore_empty | Ignores empty fields when parsing CSV/TSV files | Boolean | optional, defaults to `true` |
| extra_args | A [configuration object](https://www.npmjs.com/package/json2csv#available-options) used to pass in any extra csv parsing arguments | Object | optional, defaults to `{}` |

## Advanced Configuration

### `format`

#### json

`json` format treats every file as a single JSON record, so all files **MUST ONLY CONSIST OF A SINGLE RECORD OR ARRAY OF JSON RECORDS**. The reader will automatically detect whether the file is a record or array of records, and if it is an array of records, the reader will return a data entity for each record. This setting will tell the execution controller to ignore the `size` parameter and will provide one full file for every slice.

#### ldjson

`ldjson` format will treat files as a set of line-delimited JSON records. line  delimiters other than `\n` can be used, but the `line_delimiter` option must be set in this case.

##### tsv

`tsv` format will treat files as a set of tab-delimited values. If using the `tsv` input format, the **FIELDS OPTION MUST BE PROVIDED AS WELL**. As with `ldjson`, a custom line delimiter can be used with the `line_delimiter` parameter. Providing `tsv` as the format is the same as providing the `csv` option with `\t` as the `field_delimiter`.

#### csv

`csv` format will treat files as a set of values delimited by the `field_delimiter` option. `field_delimiter` defaults to `,`, but if multi-character or custom delimiters are needed, `csv` should be selected here and used in conjunction with the `field_delimiter` option. **FIELDS OPTION MUST BE PROVIDED AS WELL.** Custom line delimiters can be used with `line_delimiter`

#### raw

`raw` format will generate files where each line is the value of the `data` attribute of a data entity in the slice. This is mainly used to process binary data or other data that are not strings, the records must be sent to the `hdfs_exporter` in the form of:

```json
{ "data": "some processed data string or buffer" }
```
