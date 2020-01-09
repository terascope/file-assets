# s3_exporter

The `s3_exporter` will export slices to objects in S3. This exporter will ignore empty slices to prevent feeding empty objects into the S3 store.

# Optional Routing Override

If record metadata includes a `routingPath` attribute, this will override the `path` provided in this configuration.

# Options

## `path`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any valid S3 bucket/prefix name | `null` | Y |

The bucket and optional prefix for data. If there is no `/` in this parameter, it will just be treated as a bucket name, and if there is no trailing `/`, any portion of the path that isn't the bucket will be treated as the object prefix.

## `extension`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| String | `''` | N |

Optional file extension to add to file names. A `.` is not automatically prepended to this value when being added to the filename.

## `connection`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any valid S3 connector | `null` | Y |

This is the name of the S3 connector defined in Terafoundation.

## `compression`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| none, lz4, gzip | `none` | N |

Compression type to use with objects.

## `fields`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Array containing record fields | `[]` | N |

This is an optional setting that will filter and order the fields in the output. It will work with `csv`, `tsv`, and `ldjson` output formats, and if not specified, all fields will be included in the output.

## `field_delimiter`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any string | `,` | N |

Any string can be used as a delimiter for the exporter. This allows for multi-character or custom delimiters. **This option is only used with the `csv` output.** See the notes on the `format` option for more information.

## `line_delimiter`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any string | `,` | N |

Any string can be used as a delimiter for the exporter. This allows for multi-character or custom delimiters. **This option is only used with the `csv` output.** See the notes on the `format` option for more information.

## `file_per_slice`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| 'true' | `true` | N |

This processor currently only supports creating a single object for each slice. A future improvement will be to utilize multi-part uploads to allow workers to write larger batches of data to objects.

## `include_header`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| 'true', 'false' | `false` | N |

Determines whether or not to include column headers for the fields in output files. If set to `true`, a header will be added as the first entry to every file created. This option is only used for `tsv` and `csv` formats.

## `format`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| 'json', 'ldjson', 'raw', 'csv', 'tsv' | `ldjson` | N |

### json

`json` output treats each slice as an array of JSON records. It will coerce the `object_per_slice` to `true` so that each output object will be a single array that consists of all the records included in the slice. Additionally, a `line_delimiter` is appended to the end of every file.

### ldjson

`ldjson` (line-delimited JSON) output treats each slice as a set of  JSON records and will add them to objects delimited with `line_delimiter`

### tsv

`tsv` output will generate files where each line consists of tab-separated fields from each record. Providing this option is the same as providing the `csv` option with `\t` as the `field_delimiter`.

### csv

`csv` output will generate objects where each line consisting of fields from each record delimited by the delimiter specified by `delimiter`. `delimiter` defaults to `,`, but if multi-character or custom delimiters are needed, `csv` should be selected here and used in conjunction with the `delimiter` option

### raw

`raw` output will generate objects where each line is the value of the `data` attribute of a data entity in the slice. So, when using the `raw` format output, the records must be sent to the `s3_exporter` in the form of:

```json
{ "data": "some processed data string" }
```

# Example Job

This test job will generate 500k records and put them into tab-delimited files that include column headers in the worker's `/app/data/testfiles` directory. (Since the `elasticsearch_data_generator` breaks records into batches of 10k records, this will result in 50 `test_*` tsv files)

```json
{
  "name": "file_exporter",
  "lifecycle": "once",
  "workers": 1,
  "max_retries": 0,
  "operations": [
    {
      "_op": "elasticsearch_data_generator",
      "size": 500000
    },
    {
      "_op": "file_exporter",
      "path": "/app/data/testfiles",
      "file_prefix": "test",
      "format": "tsv",
      "file_per_slice": true,
      "include_header": true
    }
  ],
  "assets": [
    "file-assets"
  ]
}
```
