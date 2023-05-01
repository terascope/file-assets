# s3_sender_api

The `s3_sender_api` will provide a factory that can create file sender apis that can be accessed in any operation through the `getAPI` method on the operation.

This is a [Factory API](https://terascope.github.io/teraslice/docs/packages/job-components/api/interfaces/apifactoryregistry), which can be used to fully manage api creation and configuration.

This api is the core of the [s3_exporter](./s3_exporter.md). This contains all the same behavior, functionality and configuration of that exporter

If you are using the asset version >= 2.4.0, it should be used on >= teraslice v84.0

## Usage

### Example Processor using a s3 sender api
This is an example of a custom processor using the s3_sender_api.

Example Job

```json
{
    "name" : "testing",
    "workers" : 1,
    "slicers" : 1,
    "lifecycle" : "once",
    "assets" : [
        "file"
    ],
    "apis" : [
        {
            "_name": "s3_sender_api",
            "path": "/app/data/test_files",
            "format": "ldjson",
            "line_delimiter": "\n",
            "file_per_slice": true,
            "fields": ["name"]
        }
    ],
    "operations" : [
        {
            "_op" : "test-reader"
        },
        {
            "_op" : "some_sender",
            "api_name" : "s3_sender_api"
        }
    ]
}
```

Here is a custom processor for the job described above

```javascript
export default class SomeSender extends BatchProcessor {

    async initialize() {
        await super.initialize();
        const apiName = this.opConfig.api_name;
        const apiManager = this.getAPI(apiName);
        this.api = await apiManager.create(apiName);
    }

    async onBatch(slice) {
        await this.api.send(slice);
        // it is best practice to return the slice for any processors after this operation
        return slice;
    }
}
```

### S3 Sender Factory API Methods

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

this will create an instance of a [sender api](#s3_sender_instance), and cache it with the name given. Any config provided in the second argument will override what is specified in the apiConfig and cache it with the name provided. It will throw an error if you try creating another api with the same name parameter

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

## Example of using the factory methods in a processor
```javascript
// example of api configuration
const apiConfig = {
    _name: 's3_sender_api',
    path: '/app/data/test_files',
    format: 'ldjson',
    line_delimiter: '\n',
    file_per_slice: true,
    fields: ['name']
}

const apiManager = this.getAPI<ElasticReaderFactoryAPI>(apiName);

apiManager.size() === 0

// this will return an api cached at "normalClient" and it will use the default api config
const normalClient = await apiManager.create('normalClient', {})

apiManager.size() === 1

apiManager.get('normalClient') === normalClient

// this will return an api cached at "overrideClient"
const overrideClient = await apiManager.create('overrideClient', { path: 'other/path', format: 'tsv' })

apiManager.size() === 2

// this will return the full configuration for this client
apiManger.getConfig('overrideClient') === {
    _name: 's3_sender_api',
    path: 'other/path',
    format: 'tsv',
    line_delimiter: '\n',
    file_per_slice: true,
    fields: ['name']
}


await apiManger.remove('normalClient');

apiManager.size() === 1

apiManager.get('normalClient') === undefined
```

### S3 Sender Instance
This is the sender class that is returned from the create method of the APIFactory. This returns a [sender api](https://terascope.github.io/teraslice/docs/packages/job-components/api/interfaces/routesenderapi), which is a common interface used for sender apis.


### send (async)
```(records: DataEntities[]) => Promise<void>```
This method will send the records to S3 file

parameters:
- records: an array of data-entities

### verify (async)
```(route?: string) => Promise<void>```
This method ensures that the index is created. However, this currently is a noop as the bulk index request will make the index, this might change in the future. This exists because this follows a common interface. Other senders might need to verify that the destination exists before sending data.

parameters:
- route: a string representing the index to create

### Usage of the elasticsearch sender instance
```js
await api.send([
    DataEntity.make({
        some: 'data',
        name: 'someName',
        job: 'to be awesome!'
    })
]);
```

## Parameters
| Configuration   | Description                                                                                                                                                                                                                                                                                                 | Type     | Notes                                                                                                                                                                      |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| \_op            | Name of operation, it must reflect the exact name of the file                                                                                                                                                                                                                                               | String   | required                                                                                                                                                                   |
| path            | The bucket and optional prefix for data. If there is no `/` in this parameter, it will just be treated as a bucket name, and if there is no trailing `/`, one will be added to separate anything after the bucket from the worker names.                                                                    | String   | required, files will be named after the TS workers, so multiple workers can write data to the same directory concurrently. If there is no trailing `/`, one will be added. |
| extension       | Optional file extension to add to file names                                                                                                                                                                                                                                                                | String   | optional, A `.` is not automatically prepended to this value when being added to the filename, if it is desired it must be specified on the extension                      |
| compression     | you may specify a compression algorithm to apply to the data before being written to file, it may be set to `none`, `lz4` or `gzip`                                                                                                                                                                         | String   | optional, defaults `none`                                                                                                                                                  |
| fields          | a list of allowed fields to output. This parameter will be ignored if `format` is set to `raw`                                                                                                                                                                                                              | String[] | optional, by default all fields will be included in output                                                                                                                 |
| field_delimiter | A delimiter between field names. This is only used when `format` is set to `csv`                                                                                                                                                                                                                            | String   | optional, defaults to `,`                                                                                                                                                  |
| line_delimiter  | A delimiter applied between each record or slice, please reference the [format](#format) section for more information how this deliminator is applied for each format.                                                                                                                                      | String   | optional, defaults to `\n`                                                                                                                                                 |
| file_per_slice  | This setting determines if the output for a worker will be in a single file (`false`), or if the worker will create a new file for every slice it processes  (`true`). If set to `true`, an integer, starting at 0, will be appended to the filename and incremented by 1 for each slice a worker processes | Boolean  | optional, defaults to `true`. If using `json` format, this option will be overridden to `true`                                                                             |
| include_header  | Determines whether or not to include column headers for the fields in output files. If set to `true`, a header will be added as the first entry to every file created. This option is only used for `tsv` and `csv` formats                                                                                 | Boolean  | optional, defaults to `false`                                                                                                                                              |
| concurrency     | The represents the limit on how many parallel writes will occur at a given time                                                                                                                                                                                                                             | Number   | optional, defaults to `10`                                                                                                                                                 |
| format          | Used to determine how the data should be written to file, options are: `json`, `ldjson`, `raw`, `csv`, `tsv`                                                                                                                                                                                                | String   | required, please reference the [format](#format) section for more information                                                                                              |
| connection      | Name of the s3 connection to use when sending data                                                                                                                                                                                                                                                          | String   | optional, defaults to the `default` connection                                                                                                                             |


## Advanced Configuration

### `format`
Format determines how the data is saved to file, please check the references below for further information on each behavior.

#### json
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
