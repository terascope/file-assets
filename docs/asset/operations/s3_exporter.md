# s3_exporter

The `s3_exporter` is a processor that will export data to S3. This exporter will ignore empty slices to prevent feeding empty objects into the S3 store.

For this processor to run, a path is required in the configuration. The base bucket of the path must already exists in S3.

If you are using the asset version >= 2.4.0, it should be used on teraslice >= v84.0

## Usage

### Write ldjson to file and restrict fields

This is an example of converting the input data into ldjson and sent to the worker's `/app/data/test_files` directory. Since fields is specified, only the fields listed will be allowed through. Since file_per_slice is set to true, each slice will create a new file, which is the workerId as well as the slice order number.

Example Job

```json
{
  "name": "s3_exporter",
  "lifecycle": "once",
  "workers": 1,
  "max_retries": 0,
  "assets": [
    "file",
    "standard"
  ],
    "apis": [
    {
      "_name": "s3_sender_api",
      "path": "/app/data/test_files",
      "format": "ldjson",
      "line_delimiter": "\n",
      "file_per_slice": true,
      "fields": ["name"]
    }
  ],
  "operations": [
    {
      "_op": "test-reader"
    },
    {
      "_op": "s3_exporter",
      "_api_name": "s3_sender_api"
    }
  ]
}
```

Here is a representation of what the processor will do with the configuration listed in the job above

```javascript
const firstSlice = [
    { name: 'chilly', age: 33 },
    { name: 'willy', age: 31 },
    { name: 'philly', age: 43 },
];

const results = await process.run(firstSlice);

// the processor will always return the input records.
results === firstSlice;

// file made at bucket /app/data/test_files/{WORKER_ID}.0
`
"{"name":"chilly"}"\n
"{"name":"willy"}"\n
"{"name":"philly"}"\n
`

const secondSlice = [
    { name: 'fred', age: 33 },
    { name: 'art', age: 31 },
    { name: 'herbert', age: 43 },
];

const results = await process.run(secondSlice);

// the processor will always return the input records.
results === secondSlice;

// file made at bucket /app/data/test_files/{WORKER_ID}.1
`
"{"name":"fred"}"\n
"{"name":"art"}"\n
"{"name":"herbert"}"\n
`
```

### Write csv to file

This test job will send data to csv files that
include column headers in the worker's `/app/data/test_files` directory.

Example Job

```json
{
  "name": "s3_exporter",
  "lifecycle": "once",
  "workers": 1,
  "max_retries": 0,
  "assets": [
    "file",
    "standard"
  ],
  "apis": [
    {
      "_name": "s3_sender_api",
      "path": "/app/data/test_files",
      "format": "csv",
      "include_header": true
    }
  ],
  "operations": [
    {
      "_op": "test-reader"
    },
    {
      "_op": "s3_exporter",
      "_api_name": "s3_sender_api"
    }
  ]
}
```

Here is a representation of what the processor will do with the configuration listed in the job above

```javascript
const firstSlice = [
    { name: 'chilly', age: 33 },
    { name: 'willy', age: 31 },
    { name: 'philly', age: 43 },
];

const results = await process.run(firstSlice);

// the processor will always return the input records.
results === firstSlice;

// file made at bucket /app/data/test_files/{WORKER_ID}.0
`
"name","age"\n
"chilly", 33"\n
"willy", 31"\n
"philly", 43"\n
`
```

## Parameters

| Configuration   | Description | Type     | Notes |
| --------------- | ---------| -------- | ---------- |
| \_op            | Name of operation, it must reflect the exact name of the file | String   | required |
| path            | The bucket and optional prefix for data. If there is no `/` in this parameter, it will just be treated as a bucket name, and if there is no trailing `/`, one will be added to separate anything after the bucket from the worker names. | String   | optional, if path is not provided in the opConfig, it must be provided in the api configuration, files will be named after the TS workers, so multiple workers can write data to the same directory concurrently. |
| extension       | Optional file extension to add to file names | String   | optional, A `.` is not automatically prepended to this value when being added to the filename, if it is desired it must be specified on the extension |
| compression     | you may specify a compression algorithm to apply to the data before being written to file, it may be set to `none`, `lz4` or `gzip` | String   | optional, defaults `none` |
| fields          | a list of allowed fields to output. This parameter will be ignored if `format` is set to `raw` | String[] | optional, by default all fields will be included in output |
| _api_name        | Name of api used for s3_exporter | String   | required |
| field_delimiter | A delimiter between field names. This is only used when `format` is set to `csv` | String   | optional, defaults to `,` |
| line_delimiter  | A delimiter applied between each record or slice, please reference the [format](#format) section for more information how this deliminator is applied for each format. | String   | optional, defaults to `\n` |
| file_per_slice  | This setting determines if the output for a worker will be in a single file (`false`), or if the worker will create a new file for every slice it processes  (`true`). If set to `true`, an integer, starting at 0, will be appended to the filename and incremented by 1 for each slice a worker processes | Boolean  | optional, defaults to `true`. If using `json` format, this option will be overridden to `true` |
| include_header  | Determines whether or not to include column headers for the fields in output files. If set to `true`, a header will be added as the first entry to every file created. This option is only used for `tsv` and `csv` formats | Boolean  | optional, defaults to `false` |
| concurrency     | The represents the limit on how many parallel writes will occur at a given time | Number   | optional, defaults to `10` |
| format          | Used to determine how the data should be written to file, options are: `json`, `ldjson`, `raw`, `csv`, `tsv` | String   | required, please reference the [format](#format) section for more information |
| connection      | Name of the s3 connection to use when sending data | String   | optional, defaults to the `default` connection |

## Advanced Configuration

### `format`

Format determines how the data is saved to file, please check the references below for further information on each behavior.

### json

`json` output treats each slice as an array of JSON records. It will coerce the object_per_slice to true so that each output object will be a single array that consists of all the records included in the slice. Additionally, a line_delimiter is appended to the end of every file.

#### ldjson

`ldjson` (line-delimited JSON) format will convert each individual record into JSON and separate each one by whats configured by the `line_delimiter` parameter

#### tsv

`tsv` format will generate files where each line consists of tab-separated fields from each record. Providing this option is the same as providing the `csv` option with `\t` as the `field_delimiter`.

#### #csv

`csv` format will generate files where each line consisting of fields from each record delimited by the delimiter specified by `field_delimiter`. `field_delimiter` defaults to `,`, but if multi-character or custom delimiters are needed, `csv` should be selected here and used in conjunction with the `field_delimiter` option

#### raw

`raw` format will generate files where each line is the value of the `data` attribute of a data entity in the slice. This is mainly used to process binary data or other data that are not strings, the records must be sent to the `hdfs_exporter` in the form of:

```json
{ "data": "some processed data string or buffer" }
```

### API usage in a job

In file_assets v4, teraslice apis must be set within the job configuration. Teraslice will no longer automatically setup the api for you. Configurations for the api should no longer be set on the operation as they will be ignored.

```json
{
    "name": "s3_sender",
    "lifecycle": "once",
    "analytics": true,
    "slicers": 1,
    "workers": 1,
    "assets": [
        "file",
        "elasticsearch"
    ],
    "apis": [
        {
            "_name": "s3_sender_api",
            "path": "routed-path-s3",
            "file_per_slice": true
        },
        {
            "_name": "elasticsearch_reader_api",
            "size": 500,
            "index": "test_index",
            "type": "events",
            "date_field_name": "created",
            "time_resolution": "ms"
        }
    ],
    "operations": [
        {
            "_op": "elasticsearch_reader",
            "_api_name": "elasticsearch_reader_api",
        },
        {
            "_op": "s3_exporter",
            "_api_name": "s3_sender_api",
          }
    ]
}
```
  