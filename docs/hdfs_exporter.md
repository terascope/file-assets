# hdfs_exporter

The `hdfs_exporter` will export slices to files in HDFS.

**This processor was migrated over from `hdfs_assets` and may not work properly. It currently does not support the append error avoidance that `hdfs_assets` included**

If a functional processor is needed, use the old [HDFS asset bundle](https://github.com/terascope/hdfs-assets)


## Parameters
| Configuration   | Description                                                                                                                                                                                                                                                                                                 | Type     | Notes                                                                                                                                                 |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| \_op            | Name of operation, it must reflect the exact name of the file                                                                                                                                                                                                                                               | String   | required                                                                                                                                              |
| path            | The path and optional prefix for files. If there is no trailing `/`, latter portion of the path will be treated as the object prefix.                                                                                                                                                                       | String   | optional, if path is not provided in the opConfig, it must be provided in the api configuration                                                       |
| extension       | Optional file extension to add to file names                                                                                                                                                                                                                                                                | String   | optional, A `.` is not automatically prepended to this value when being added to the filename, if it is desired it must be specified on the extension |
| compression     | you may specify a compression algorithm to apply to the data before being written to file, it may be set to `none`, `lz4` or `gzip`                                                                                                                                                                         | String   | optional, defaults `none`                                                                                                                             |
| fields          | a list of allowed fields to output. This parameter will be ignored if `format` is set to `raw`                                                                                                                                                                                                              | String[] | optional, by default all fields will be included in output                                                                                            |
| api_name        | Name of api used for hdfs_exporter                                                                                                                                                                                                                                                                          | String   | optional, defaults to `hdfs_sender_api`                                                                                                               |
| field_delimiter | A delimiter between field names. This is only used when `format` is set to `csv`                                                                                                                                                                                                                            | String   | optional, defaults to `,`                                                                                                                             |
| line_delimiter  | A delimiter applied between each record or slice, please reference the [format](#format) section for more information how this deliminator is applied for each format.                                                                                                                                      | String   | optional, defaults to `\n`                                                                                                                            |
| file_per_slice  | This setting determines if the output for a worker will be in a single file (`false`), or if the worker will create a new file for every slice it processes  (`true`). If set to `true`, an integer, starting at 0, will be appended to the filename and incremented by 1 for each slice a worker processes | Boolean  | optional, defaults to `true`. If using `json` format, this option will be overridden to `true`                                                        |
| include_header  | Determines whether or not to include column headers for the fields in output files. If set to `true`, a header will be added as the first entry to every file created. This option is only used for `tsv` and `csv` formats                                                                                 | Boolean  | optional, defaults to `false`                                                                                                                         |
| concurrency     | The represents the limit on how many parallel writes will occur at a given time                                                                                                                                                                                                                             | Number   | optional, defaults to `10`                                                                                                                            |
| format          | Used to determine how the data should be written to file, options are: `json`, `ldjson`, `raw`, `csv`, `tsv`                                                                                                                                                                                                | String   | required, please reference the [format](#format) section for more information                                                                         |
| connection      | Name of the hdfs connection to use when sending data                                                                                                                                                                                                                                                        | String   | optional, defaults to the `default` connection                                                                                                        |


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
In file_assets v1, many core components were made into teraslice apis. When you use a file processor it will automatically setup the api for you, but if you manually specify the api, then there are restrictions on what configurations you can put on the operation so that clashing of configurations are minimized. The api configs take precedence.

If submitting the job in long form, here is a list of parameters that will throw an error if also specified on the opConfig, since these values should be placed on the api:
- `path`


`SHORT FORM (no api specified)`
```json
{
  "name": "hdfs_exporter",
  "lifecycle": "once",
  "workers": 1,
  "max_retries": 0,
  "assets": [
    "file",
    "standard"
  ],
  "operations": [
    {
      "_op": "data_generator",
      "size": 500000
    },
    {
      "_op": "hdfs_exporter",
      "path": "/app/data/test_files",
      "format": "ldjson",
      "file_per_slice": true,
      "include_header": true
    }
  ]
}
```

this configuration will be expanded out to the long form underneath the hood
`LONG FORM (api is specified)`

```json
{
  "name": "hdfs_exporter",
  "lifecycle": "once",
  "workers": 1,
  "max_retries": 0,
  "assets": [
    "file",
    "standard"
  ],
  "apis": [
      {
          "_name": "hdfs_sender_api",
          "path": "/app/data/test_files",
          "format": "ldjson",
          "file_per_slice": true,
          "include_header": true
      }
  ],
  "operations": [
    {
      "_op": "data_generator",
      "size": 500000
    },
    {
      "_op": "hdfs_exporter",
      "api_name": "file_sender_api"
    }
  ]
}
```
