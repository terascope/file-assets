# s3_reader

The `s3_reader` can fetch data from an S3 bucket. It works by slicing up the file into byte chunks that encompass complete records determined by the format parameter so that they can be independently read from the file.

For this processor to run, a path is required in the configuration. All intermediate directories must pre-exist, and the workers will need to have adequate permissions to read from that directory.

This reader currently only works in `once` jobs.

If you are using the asset version >= 2.4.0, it should be used on teraslice >= v84.0

## Usage

### Read ldjson from a file

This test job will find and read the files in the `/app/data/test_files`. In this example, the TS cluster could be a single-node cluster or a multi-node cluster where `/app/data` is directory shared between all the workers.

 Example Job

```json
{
  "name": "s3_reader",
  "lifecycle": "once",
  "workers": 10,
  "max_retries": 0,
   "assets": [
    "file"
  ],
  "operations": [
    {
        "_op": "s3_reader",
        "path": "/app/data/test_files",
        "size": 100000,
        "format": "ldjson"
    },
    {
        "_op": "noop"
    }
  ]
}
```

Here is a representation of what the processor will do with the configuration listed in the job above

```javascript
const slice =  {
    path: '/app/data/test_files/someFile.txt',
    offset: 0,
    total: 364,
    length: 364
}

const results = await reader.run(slice);

results === [{ some: 'records'}, { more: 'data' }]
```

## Parameters

| Configuration   | Description                                                                                                                                                                                                                                                                                                 | Type     | Notes                                                                                                                                                 |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| \_op            | Name of operation, it must reflect the exact name of the file                                                                                                                                                                                                                                               | String   | required                                                                                                                                              |
| _api_name        | Name of api used for s3_reader                                                                                                                                                                                                                                                                              | String   | required                                                                                                                 |

### API usage in a job

In file_assets v4, teraslice apis must be set within the job configuration. Teraslice will no longer automatically setup the api for you. All fields related to the api that were previously allowed on the operation config must be specified in the api config. Configurations for the api should no longer be set on the operation as they will be ignored. The api's `_name` must match the operation's `_api_name`.

```json
{
  "name": "s3_reader",
  "lifecycle": "once",
  "workers": 1,
  "max_retries": 0,
  "assets": [
    "file",
    "standard"
  ],
  "apis": [
      {
          "_name": "s3_reader_api",
          "path": "/app/data/test_files",
          "format": "tsv",
          "file_per_slice": true,
          "include_header": true,
          "size": 100000
      }
  ],
  "operations": [
    {
      "_op": "s3_reader",
      "_api_name": "s3_reader_api"
    },
    {
        "_op": "noop"
    }
  ]
}
```
