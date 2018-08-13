# file-assets
Teraslice processors for working with data stored in files on disk.

# Parameters

| Name | Description | Default | Required |
| ---- | ----------- | ------- | -------- |
| path | Directory where data will be saved. All intermediate directories must pre-exist.' | | Y |
| file_prefix | A prefix to prepend to the file | 'export' | N |
| fields | Array containing specific fields to include in the output | All fields | N |
| delimiter | Character to use for separating fields in the output. | ',' | N |
| file_per_slice | Determines if a new file is created for each slice | false | N |
| include_header | Determines if the names of the fields are included as the first line of the output file | false | N |  
| format | Determines the format of the output file | N | json |

- If a custom delimiter is needed (one other than a tab or comma), set the `delimiter` option to the
  desired delimiter and set the `format` option to `csv`.

# Example Job  

This test job will generate 5k records, and then put them into tab-delimited files in the
`/app/data/testfiles` directory

```
{
  "name": "csv_exporter",
  "lifecycle": "persistent",
  "workers": 1,
  "max_retries": 0,
  "operations": [
    {
      "_op": "elasticsearch_data_generator",
      "size": 5000
    },
    {
      "_op": "csv_exporter",
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
