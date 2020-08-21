# hdfs_reader

The `hdfs_reader` will export slices to files in HDFS.

**This processor was migrated over from `hdfs_assets` and may not work properly. It currently does not support the append error avoidance that `hdfs_assets` included**

If a functional processor is needed, use the old [HDFS asset bundle](https://github.com/terascope/hdfs-assets)


## Parameters
| Configuration | Description | Type |  Notes |
| --------- | -------- | ------ | ------ |
| \_op| Name of operation, it must reflect the exact name of the file | String | required |
| path | The bucket and optional prefix for data. If there is no `/` in this parameter, it will just be treated as a bucket name, and if there is no trailing `/`, any portion of the path that isn't the bucket will be treated as the object prefix. If path is not provided in the opConfig, it must be provided in the api configuration. | String | optional, if path is not provided in the opConfig, it must be provided in the api configuration |
| extension | Optional file extension to add to file names | String | optional, A `.` is not automatically prepended to this value when being added to the filename, if it is desired it must be specified on the extension |
| compression | you may specify a compression algorithm to apply to the data before being written to file, it may be set to `none`, `lz4` or `gzip` | String | optional, defaults `none` |
| fields | a list of all field names present in the file **in the order that they are found**, this essentially acts as the headers. This option is only used for `tsv` and `csv` formats | String[] | optional |
| api_name | Name of api used for hdfs_reader | String | optional, defaults to `hdfs_reader_api` |
| field_delimiter | A delimiter between field names. This is only used when `format` is set to `csv`  | String | optional, defaults to `,`  |
| line_delimiter | If a line delimiter other than `\n` is used in the files, this option will tell the reader how to read records in the file. This option is ignored for `json` format. See the [format](#format) section for more information how this deliminator is applied for each format. | String | optional, defaults to `\n` |
| file_per_slice | This setting determines if the output for a worker will be in a single file (`false`), or if the worker will create a new file for every slice it processes  (`true`). If set to `true`, an integer, starting at 0, will be appended to the filename and incremented by 1 for each slice a worker processes | Boolean | optional, defaults to `true`. If using `json` format, this option will be overridden to `true` |
| include_header | Determines whether or not to include column headers for the fields in output files. If set to `true`, a header will be added as the first entry to every file created. This option is only used for `tsv` and `csv` formats | Boolean | optional, defaults to `false` |
| format | Used to determine how the data should be written to file, options are: `json`, `ldjson`, `raw`, `csv`, `tsv` | String | optional, defaults to `ldjson`, please reference the [format](#format) section for more information |
| size | Determines the target slice size in bytes. The actual slice size will vary slightly since the reader will read additional bytes from the file in order to complete a record if the read ends with a partial record. This option is ignored for `json` format. See `json` format option below for more info. | Number | optional, defaults to `10000000` |
| remove_header | Checks for the header row in csv or tsv files and removes it | Boolean | optional, defaults to `true` |
| ignore_empty | Ignores empty fields when parsing CSV/TSV files | Boolean | optional, defaults to `true` |
| extra_args | A [configuration object](https://www.npmjs.com/package/json2csv#available-options) used to pass in any extra csv parsing arguments | Object | optional, defaults to `{}` |
| user | A valid HDFS user to use when reading the files | String | optional, defaults to `hdfs` |
| connection | Name of the hdfs connection to use when sending data | String | optional, defaults to the `default` connection |

## Advanced Configuration

## `format`

### json

`json` format treats every file as a single JSON record, so all files **MUST ONLY CONSIST OF A SINGLE RECORD OR ARRAY OF JSON RECORDS**. The reader will automatically detect whether the file is a record or array of records, and if it is an array of records, the reader will return a data entity for each record. This setting will tell the execution controller to ignore the `size` parameter and will provide one full file for every slice.

### ldjson

`ldjson` format will treat files as a set of line-delimited JSON records. line  delimiters other than `\n` can be used, but the `line_delimiter` option must be set in this case.

### tsv

`tsv` format will treat files as a set of tab-delimited values. If using the `tsv` input format, the **FIELDS OPTION MUST BE PROVIDED AS WELL**. As with `ldjson`, a custom line delimiter can be used with the `line_delimiter` parameter. Providing `tsv` as the format is the same as providing the `csv` option with `\t` as the `field_delimiter`.

### csv

`csv` format will treat files as a set of values delimited by the `field_delimiter` option. `field_delimiter` defaults to `,`, but if multi-character or custom delimiters are needed, `csv` should be selected here and used in conjunction with the `field_delimiter` option. **FIELDS OPTION MUST BE PROVIDED AS WELL.** Custom line delimiters can be used with `line_delimiter`

### raw

`raw` format will treat files as a set of raw string separated by the `line_delimiter`, and each string will be stored in the `data` attribute of a data entity. The reader will make sure slices split on the `line_delimiter` so partial lines do not show up in records.

#### API usage in a job
In file_assets v1, many core components were made into teraslice apis. When you use a file processor it will automatically setup the api for you, but if you manually specify the api, then there are restrictions on what configurations you can put on the operation so that clashing of configurations are minimized. The api configs take precedence.

If submitting the job in long form, here is a list of parameters that will throw an error if also specified on the opConfig, since these values should be placed on the api:
- `path`


`SHORT FORM (no api specified)`
```json
{
  "name": "hdfs_reader",
  "lifecycle": "once",
  "workers": 1,
  "max_retries": 0,
  "assets": [
    "file",
    "standard"
  ],
  "operations": [
    {
      "_op": "hdfs_reader",
      "path": "/app/data/test_files",
      "format": "ldjson",
      "size": 100000
    },
    {
        "_op": "noop"
    }
  ]
}
```

this configuration will be expanded out to the long form underneath the hood
`LONG FORM (api is specified)`

```json
{
  "name": "hdfs_reader",
  "lifecycle": "once",
  "workers": 1,
  "max_retries": 0,
  "assets": [
    "file",
    "standard"
  ],
  "apis": [
      {
          "_name": "hdfs_reader_api",
          "path": "/app/data/test_files",
          "format": "ldjson",
          "size": 100000
      }
  ],
  "operations": [
    {
      "_op": "hdfs_reader",
      "api_name": "hdfs_reader_api"
    },
    {
        "_op": "noop"
    }
  ]
}
```
