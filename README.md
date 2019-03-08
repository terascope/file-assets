# file-assets

> Teraslice processors for working with data stored in files on disk.

## file_exporter

| Name           | Description                                                                             | Default    | Required |
| -------------- | --------------------------------------------------------------------------------------- | ---------- | -------- |
| path           | Directory where data will be saved. All intermediate directories must pre-exist.'       |            | Y        |
| file_prefix    | A prefix to prepend to the file                                                         | 'export'   | N        |
| fields         | Array containing specific fields to include in the output                               | All fields | N        |
| delimiter      | Character to use for separating fields in the output.                                   | ','        | N        |
| file_per_slice | Determines if a new file is created for each slice                                      | false      | N        |
| include_header | Determines if the names of the fields are included as the first line of the output file | false      | N        |
| format         | Determines the format of the output file                                                | N          | json     |


- If a custom delimiter is needed (one other than a tab or comma), set the `delimiter` option to the
  desired delimiter and set the `format` option to `csv`.

# Example Job  

This test job will generate 5k records, and then put them into tab-delimited files in the
`/app/data/testfiles` directory

```json
{
  "name": "file_exporter",
  "lifecycle": "persistent",
  "workers": 1,
  "max_retries": 0,
  "operations": [
    {
      "_op": "elasticsearch_data_generator",
      "size": 5000
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


## file_reader

| Name      | Description                                                                                                                                     | Default | Required |
| --------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | ------- | -------- |
| path      | Directory where data will be located. The contents of the directory must contain at least one file and can be a mix of files and subdirectories |         | Y        |
| delimiter | Record delimiter used in the file. Currently only supports `\n`                                                                                 | `\n`    | N        |
| size      | Target slice size in bytes. The reader will adjust this a little bit to ensure there are no partial records at slice boundaries                 | 100000  | N        |
| format    | Determines the format of the output file. Currently only supports `json` and `raw`                                                              | N       | json     |


This processor is ONLY intended for `once` jobs at this time.

# Example Job  

This test job will find and read the files in the `/app/data/testfiles`, and then put them into ES.
The directory has this structure:  
```
/app/data/testfiles
├── test_data1_200k_records.txt
├── test_data2_200k_records.txt
└── subdir
    ├── test_data3_200k_records.txt
    └── test_data4_200k_records.txt
```

```json
{
  "name": "file_reader",
  "lifecycle": "once",
  "workers": 10,
  "max_retries": 0,
  "operations": [
  {
    "_op": "file_reader",
    "path": "/app/data/testfiles",
    "size": 100000,
    "format": "json"
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
    "file-assets",
    "elasticsearch"
  ]
}
```

The result will be the ES index `zb-test_records` with 800k records.
