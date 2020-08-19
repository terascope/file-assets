# file_sender_api

The `file_sender_api` will provide a factory that can create file sender apis that can be accessed in any operation through the `getAPI` method on the operation.


This is a [Factory API](https://terascope.github.io/teraslice/docs/packages/job-components/api/interfaces/apifactoryregistry), which can be used to fully manage api creation and configuration.

## File Sender Factory API Methods

### size

this will return how many separate sender apis are in the cache

### get
parameters
- name: String

this will fetch any sender api that is associated with the name provided

### getConfig
parameters
- name: String

this will fetch any sender api config that is associated with the name provided

### create (async)
parameters
- name: String
- configOverrides: Check options below, optional

this will create an instance of a sender api, and cache it with the name given. Any
config provided in the second argument will override what is specified in the apiConfig and cache it with the name provided. It will throw an error if you try creating another api with the same name parameter

```typescript
    const apiManager = this.getAPI<FileSenderFactoryAPI>(apiName);
    // this will return an api cached at "normalClient" and this instance will use all configurations listed on the apiConfig
    const client = apiManager.create('normalClient')

    // this will return an api cached at "overrideClient" and this instance will have an override setting the parameter compression to "gzip", this will use the rest of the configurations listed in the apiConfig
    const overrideClient = apiManager.create('overrideClient', { compression: 'gzip'})
```

### remove (async)
parameters
- name: String

this will remove an instance of a sender api from the cache and will follow any cleanup code specified in the api code.

### entries

This will allow you to iterate over the cache name and client of the cache

### keys

This will allow you to iterate over the cache name of the cache

### values

This will allow you to iterate over the clients of the cache


## File Sender Instance
This is the sender class that is returned from the create method of the APIFactory

### send
```(records: DataEntity[]) => Promise<void>```
parameters
- records: DataEntity[]

This method will send the records to file

```js
    const docs = [ DataEntity.make({ some: 'data' })]
    await api.send(docs)
```

### verify
```(route?: string) => Promise<void>```
parameters
- route: string, optional, is any dirPath past what is specified in the path config that is listed in the Options

This method makes sure the file directory structure is present, use the before sending to make sure the dir exists.

```js
api.verify() // this makes sure the path config provided is available
api.verify('other/path') // this makes sure "{CONFIG_PATH}/other/path" is available
```

## Options

### `path`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any valid path | `null` | Y |

This is the directory where data will be saved. All intermediate directories must pre-exist, and the directory must be accessible by the TS workers. Files will be named after the TS workers, so multiple workers can write data to the same directory concurrently. If there is no trailing `/`, one will be added.

### `extension`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| String | `''` | N |

Optional file extension to add to file names. A `.` is not automatically prepended to this value when being added to the filename.

### `compression`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| none, lz4, gzip | `none` | N |

Compression type to use with files.

### `fields`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Array containing record fields | `[]` | N |

This is an optional setting that will filter and order the fields in the output. It will work with `csv`, `tsv`, and `ldjson` output formats, and if not specified, all fields will be included in the output.


### `field_delimiter`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any string | `,` | N |

Any string can be used as a delimiter for the exporter. This allows for multi-character or custom delimiters. **This option is only used with the `csv` output.** See the notes on the `format` option for more information.

### `line_delimiter`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any string | `\n` | N |

Any string can be used as a delimiter for the exporter. This allows for multi-character or custom delimiters. **This option is only used with the `csv` output.** See the notes on the `format` option for more information.

### `file_per_slice`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| 'true', 'false' | `true` | N |

This setting determines if the output for a worker will be in a single file (`false`), or if the worker will create a new file for every slice it processes  (`true`). If set to `true`, an integer, starting at 0, will be appended to the filename and incremented by 1 for each slice a worker processes. **If using `json` format, this option will be overridden to `true`.** See format notes below for more information.

### `include_header`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| 'true', 'false' | `false` | N |

Determines whether or not to include column headers for the fields in output files. If set to `true`, a header will be added as the first entry to every file created. This option is only used for `tsv` and `csv` formats.

### `concurrency`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any positive integer | `10` | N |

The concurrency the slicer slicer will use to write to s3

### `format`

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


### Example Processor using a file sender api
```typescript
export default class SomeBatcher extends BatchProcessor<SomeConfig> {
    api!: FileSender;

    async initialize(): Promise<void> {
        await super.initialize();
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI<FileSenderFactoryAPI>(apiName);
        this.api = await apiManager.create(apiName);
    }

    async onBatch(slice: DataEntity[]): Promise<DataEntity[]> {
        await this.api.send(slice);
        // it is best practice to return the slice for any processors after this operation
        return slice;
    }
}

```
