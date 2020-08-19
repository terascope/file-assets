# file_exporter

The `file_exporter` will export slices to files local to the Teraslice workers.


# Options

## `path`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any valid path | `null` | N |

This is the directory where data will be saved. All intermediate directories must pre-exist, and the directory must be accessible by the TS workers. Files will be named after the TS workers, so multiple workers can write data to the same directory concurrently. If there is no trailing `/`, one will be added. If path is not provided in the opConfig, it must be provided in the api configuration.

## `extension`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| String | `''` | N |

Optional file extension to add to file names. A `.` is not automatically prepended to this value when being added to the filename.

## `compression`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| none, lz4, gzip | `none` | N |

Compression type to use with files.

## `fields`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Array containing record fields | `[]` | N |

This is an optional setting that will filter and order the fields in the output. It will work with `csv`, `tsv`, and `ldjson` output formats, and if not specified, all fields will be included in the output.

## `api_name`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| String | `file_sender_api` | N |

This parameter will determine which api file sender api to use. If one is not provided, the default file_sender_api will be instantiated and hooked up to this processor.

## `field_delimiter`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any string | `,` | N |

Any string can be used as a delimiter for the exporter. This allows for multi-character or custom delimiters. **This option is only used with the `csv` output.** See the notes on the `format` option for more information.

## `line_delimiter`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any string | `\n` | N |

Any string can be used as a delimiter for the exporter. This allows for multi-character or custom delimiters. **This option is only used with the `csv` output.** See the notes on the `format` option for more information.

## `file_per_slice`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| 'true', 'false' | `true` | N |

This setting determines if the output for a worker will be in a single file (`false`), or if the worker will create a new file for every slice it processes  (`true`). If set to `true`, an integer, starting at 0, will be appended to the filename and incremented by 1 for each slice a worker processes. **If using `json` format, this option will be overridden to `true`.** See format notes below for more information.

## `include_header`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| 'true', 'false' | `false` | N |

Determines whether or not to include column headers for the fields in output files. If set to `true`, a header will be added as the first entry to every file created. This option is only used for `tsv` and `csv` formats.

## `concurrency`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any positive integer | `10` | N |

The concurrency the slicer slicer will use to write to s3

## `format`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| 'json', 'ldjson', 'raw', 'csv', 'tsv' | `ldjson` | N |

### json

`json` output treats each slice as an array of JSON records. It will coerce the `file_per_slice` to `true` so that each output file will be a single array that consists of all the records included in the slice. Additionally, a `line_delimiter` is appended to the end of every file.

### ldjson

`ldjson` (line-delimited JSON) output treats each slice as a set of  JSON records and will add them to files delimited with `line_delimiter`

### tsv

`tsv` output will generate files where each line consists of tab-separated fields from each record. Providing this option is the same as providing the `csv` option with `\t` as the `field_delimiter`.

### csv

`csv` output will generate files where each line consisting of fields from each record delimited by the delimiter specified by `delimiter`. `delimiter` defaults to `,`, but if multi-character or custom delimiters are needed, `csv` should be selected here and used in conjunction with the `delimiter` option

### raw

`raw` output will generate files where each line is the value of the `data` attribute of a data entity in the slice. So, when using the `raw` format output, the records must be sent to the `file_exporter` in the form of:

```json
{ "data": "some processed data string" }
```

# Example Job

This test job will generate 500k records and put them into tab-delimited files that include column headers in the worker's `/app/data/test_files` directory. (Since the `elasticsearch_data_generator` breaks records into batches of 10k records, this will result in 50 `test_*` tsv files)

`SHORT FORM (no api specified)`
```json
{
  "name": "file_exporter",
  "lifecycle": "once",
  "workers": 1,
  "max_retries": 0,
  "assets": [
    "file",
    "elasticsearch"
  ],
  "operations": [
    {
      "_op": "elasticsearch_data_generator",
      "size": 500000
    },
    {
      "_op": "file_exporter",
      "path": "/app/data/test_files",
      "format": "tsv",
      "file_per_slice": true,
      "include_header": true
    }
  ],
  "assets": [
    "file"
  ]
}
```

this configuration will be expanded out to the long form underneath the hood

`LONG FORM (api is specified)`
```json
{
  "name": "file_exporter",
  "lifecycle": "once",
  "workers": 1,
  "max_retries": 0,
  "assets": [
    "file",
    "elasticsearch"
  ],
  "apis": [
      {
          "_name": "file_sender_api",
          "path": "/app/data/test_files",
          "format": "tsv",
          "file_per_slice": true,
          "include_header": true
      }
  ],
  "operations": [
    {
      "_op": "elasticsearch_data_generator",
      "size": 500000
    },
    {
      "_op": "file_exporter",
      "api_name": "file_sender_api"
    }
  ],
  "assets": [
    "file"
  ]
}
```

If you specify the long form of the job (you create the api yourself and wire it up) then the "path" parameter must NOT be placed in opConfig as it is specified on the api.
