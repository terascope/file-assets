# s3_reader

The `s3_reader` will slice up and read files in an S3 bucket. It is currently only for use with `once` jobs.

# Options

## `path`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any valid S3 bucket/prefix name | `null` | Y |

The bucket and optional prefix for data. If there is no `/` in this parameter, it will just be treated as a bucket name, and anything separated from the bucket name with a `/` will be treated as a subdirectory whether or not there is a trailing `/`.

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

## `line_delimiter`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Line-delimiting string | `\n` | N |

If a line delimiter other than `\n` is used in the objects, this option will tell the reader how to read records in the objects. This option is ignored for `json` format. See `json` format option below for more info.

## `size`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Non-zero positive integer | `10000000` | N |

Determines the target slice size in bytes. The actual slice size will vary slightly since the reader will read additional bytes from the object in order to complete a record if the read ends with a partial record. This option is ignored for `json` format. See `json` format option below for more info.

## `field_delimiter`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any string | `,` | N |

Any string can be used as a delimiter for the reader. This allows for multi-character or custom delimiters. **This option is only used with the `csv` format.** See the notes on the `format` option for more information.

## `fields`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Array of field | [] | N |

Fields present in the files. This option is only used for `tsv` and `csv` formats, and it **MUST INCLUDE ALL FIELDS IN THE ORDER THEY APPEAR**.

## `file_per_slice`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| 'true', 'false' | `false` | N |

This setting determines if files will be split into multiple slices (`false`), each file will be contained in a single slice (`true`).  **If using `json` format, this option will be overridden to `true`.** See format notes below for more information.

## `remove_header`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| 'true', 'false' | `false` | N |

Determines whether or not to keep column headers when they appear in a slice. If set to `true`, the record will be set to `null` every time a header is encountered. This option is only used for `tsv` and `csv` formats.

## `format`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| 'json', 'ldjson', 'raw', 'csv', 'tsv' | `ldjson` | N |

### json

`json` format treats every object as a single JSON record, so all files **MUST ONLY CONSIST OF A SINGLE RECORD OR ARRAY OF JSON RECORDS**. The reader will automatically detect whether the object is a record or array of records, and if it is an array of records, the reader will return a data entity for each record. This setting will tell the execution controller to ignore the `size` parameter and will provide one full file for every slice.

### ldjson

`ldjson` format will treat objects as a set of line-delimited JSON records. line  delimiters other than `\n` can be used, but the `line_delimiter` option must be set in this case.

### tsv

`tsv` format will treat objects as a set of tab-delimited values. If using the `tsv` input format, the **FIELDS OPTION MUST BE PROVIDED AS WELL**. As with `ldjson`, a custom line delimiter can be used with the `line_delimiter` parameter. Providing `tsv` as the format is the same as providing the `csv` option with `\t` as the `field_delimiter`.

### csv

`csv` format will treat objects as a set of values delimited by the `field_delimiter` option. `field_delimiter` defaults to `,`, but if multi-character or custom delimiters are needed, `csv` should be selected here and used in conjunction with the `field_delimiter` option. **FIELDS OPTION MUST BE PROVIDED AS WELL.** Custom line delimiters can be used with `line_delimiter`

### raw

`raw` format will treat objects as a set of raw string separated by the `line_delimiter`, and each string will be stored in the `data` attribute of a data entity. The reader will make sure slices split on the `line_delimiter` so partial lines do not show up in records.

# Example Job

This test job will find and read the objects in the `staging` bucket, and then move them to ES.

The bucket has this structure:
```text
s3://staging
├── test_data1_200k_records.txt
├── test_data2_200k_records.txt
└── aux-data
    ├── test_data3_200k_records.txt
    └── test_data4_200k_records.txt
```

```json
{
  "name": "s3_to_es",
  "lifecycle": "once",
  "workers": 10,
  "max_retries": 0,
  "operations": [
  {
    "_op": "s3_reader",
    "path": "staging",
    "size": 100000,
    "format": "ldjson",
    "connector": "staging_s3"
  },
  {
    "_op": "elasticsearch_index_selector",
    "index": "zb-test_records",
    "type": "events"
  },
  {
    "_op": "elasticsearch_bulk",
    "size": 10000,
    "connection": "my-test-cluster"
  }
  ],
  "assets": [
    "file",
    "elasticsearch"
  ]
}
```

The result will be the ES index `zb-test_records` with 800k records.
