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
| _api_name        | Name of api used for s3_exporter | String   | required |

### API usage in a job

In file_assets v4, teraslice apis must be set within the job configuration. Teraslice will no longer automatically setup the api for you. All fields related to the api that were previously allowed on the operation config must be specified in the api config. Configurations for the api should no longer be set on the operation as they will be ignored. The api's `_name` must match the operation's `_api_name`.

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
  