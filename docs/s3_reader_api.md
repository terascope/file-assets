# s3_reader_api

The `s3_reader_api` will provide a factory that can create file reader apis that can be accessed in any operation through the `getAPI` method on the operation.


This is a [Factory API](https://terascope.github.io/teraslice/docs/packages/job-components/api/interfaces/apifactoryregistry), which can be used to fully manage api creation and configuration.

## S3 Reader Factory API Methods
### size

this will return how many seperate reader apis are in the cache

### get
parameters
- name: String

this will fetch any reader api that is associated with the name provided

### getConfig
parameters
- name: String

this will fetch any reader api config that is associated with the name provided

### create (async)
parameters
- name: String
- configOverrides: Check options below, optional

this will create an instance of a reader api, and cache it with the name given. Any
config provided in the second argument will override what is specified in the apiConfig and cache it with the name provided. It will throw an error if you try creating another api with the same name parameter

```typescript
    const apiManager = this.getAPI<S3ReaderFactoryAPI>(apiName);
    // this will return an api cached at "normalClient" and this instance will use all configurations listed on the apiConfig
    const client = apiManager.create('normalClient')

    // this will return an api cached at "overrideClient" and this instance will have an override setting the parameter compression to "gzip", this will use the rest of the configurations listed in the apiConfig
    const overrideClient = apiManager.create('overrideClient', { compression: 'gzip'})
```

### remove (async)
parameters
- name: String

this will remove an instance of a reader api from the cache and will follow any cleanup code specified in the api code.

### entries

This will allow you to iterate over the cache name and client of the cache

### keys

This will allow you to iterate over the cache name of the cache

### values

This will allow you to iterate over the clients of the cache


### S3 Reader Instance
This is the reader class that is returned from the create method of the APIFactory

### fetch
```(slice: SlicedFileResults) => Promise<string>```
parameters:
- slice: {
    path: string,
    total: number (total number of bytes),
    length: number (how many bytes to read),
    offset: number (where to start reading from)
}

This method will send the records to file

```js
    // this will read the first 500 bytes of the file
    const slice = {
        path: 'some/data/path',
        total: 10000,
        length: 500,
        offset: 0
    }
    const results = await api.read(docs)
```


## Options

### path

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any valid S3 bucket/prefix name | `null` | N |

The bucket and optional prefix for data. If there is no `/` in this parameter, it will just be treated as a bucket name, and anything separated from the bucket name with a `/` will be treated as a subdirectory whether or not there is a trailing `/`.
If path is not provided in the opConfig, it must be provided in the api configuration.

### connection

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any valid S3 connector | `default` | N |

This is the name of the S3 connector defined in Terafoundation.

### compression

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| none, lz4, gzip | `none` | N |

Compression type to use with objects.

### line_delimiter

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Line-delimiting string | `\n` | N |

If a line delimiter other than `\n` is used in the objects, this option will tell the reader how to read records in the objects. This option is ignored for `json` format. See `json` format option below for more info.

### size

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Non-zero positive integer | `10000000` | N |

Determines the target slice size in bytes. The actual slice size will vary slightly since the reader will read additional bytes from the object in order to complete a record if the read ends with a partial record. This option is ignored for `json` format. See `json` format option below for more info.

### field_delimiter

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any string | `,` | N |

Any string can be used as a delimiter for the reader. This allows for multi-character or custom delimiters. **This option is only used with the `csv` format.** See the notes on the `format` option for more information.

### fields

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Array of field | [] | N |

Fields present in the files. This option is only used for `tsv` and `csv` formats, and it **MUST INCLUDE ALL FIELDS IN THE ORDER THEY APPEAR**.

### file_per_slice

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| 'true', 'false' | `true` | N |

This setting determines if files will be split into multiple slices (`false`), each file will be contained in a single slice (`true`).  **If using `json` format, this option will be overridden to `true`.** See format notes below for more information.

### api_name

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| String | `file_reader_api` | N |

This parameter will determine which api file reader api to use. If one is not provided, the default file_reader_api will be instantiated and hooked up to this processor.

### remove_header`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| 'true', 'false' | `true` | N |

Determines whether or not to keep column headers when they appear in a slice. If set to `true`, the record will be set to `null` every time a header is encountered. This option is only used for `tsv` and `csv` formats.

### ignore_empty

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| 'true', 'false' | `true` | N |

Ignores fields without values when parsing CSV.
i.e. the row "val1,val3" will generate the record '{"field1":"val1","field3":"val3"}' if set to true.

### extra_args

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Object | `{}` | N |

An object used to pass in any extra csv parsing arguments

### format

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
