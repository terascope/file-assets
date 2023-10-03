# file-assets
> A set of Teraslice processors for working with data stored in files on disk. The readers utilize the `chunked-file-reader` module (migrated into this bundle from the Teraslice monorepo) to break data into records.

Since all the readers in this asset bundle use DataEntities, the slice's file path can be retrieved from each record by using something like `record.getMetadata('path')`. More information about DataEntities can be found [here](https://terascope.github.io/teraslice/docs/packages/utils/api/classes/dataentity).

This bundle includes the following processors:
- [`file_exporter`](./docs/file_exporter.md)
- [`file_reader`](./docs/file_reader.md)
- [`s3_exporter`](./docs/s3_exporter.md)
- [`s3_reader`](./docs/s3_reader.md)
- [`file_sender_api`](./docs/file_sender_api.md)
- [`file_reader_api`](./docs/file_reader_api.md)
- [`s3_sender_api`](./docs/s3_sender_api.md)
- [`s3_reader_api`](./docs/s3_reader_api.md)

## Releases

You can find a list of releases, changes, and pre-built asset bundles [here](https://github.com/terascope/file-assets/releases).

## Getting Started

This asset bundle requires a running Teraslice cluster, you can find the documentation [here](https://github.com/terascope/teraslice/blob/master/README.md).

```bash
# Step 1: make sure you have teraslice-cli installed
yarn global add teraslice-cli

# Step 2:
# FIXME: this should be accurate
teraslice-cli assets deploy \<cluster alias\> terascope/file-assets
```

**IMPORTANT:** Additionally make sure have installed the required [connectors](#connectors).

## Connectors
### S3 Connector

**Configuration:**

The terafoundation level S3 configuration is as follows:

| Configuration | Description | Type |  Notes |
| --------- | -------- | ------ | ------ |
| endpoint | Target S3 endpoint | String | defaults to `127.0.0.1:80`
| accessKeyId | S3 access key ID | String | required |
| secretAccessKey | S3 secret access key | String | required |
| region | AWS Region where bucket is located | String | optional, defaults to `us-east-1` |
| maxRetries | Maximum retry attempts | Number | optional, defaults to `3` |
| maxRedirects | Maximum redirects allowed | Number | optional, defaults to `10` |
| sslEnabled | Flag to enable/disable SSL communication | Boolean | optional, defaults to `true` |
| certLocation | Location of ssl cert | String | Must be provided if `sslEnabled` is true |
| forcePathStyle | Whether to force path style URLs for S3 objects | Boolean | optional, defaults to `false` |
| bucketEndpoint | ?????????????? | Boolean | optional, defaults to `false` |

**Terafoundation S3 configuration example:**

```yaml
terafoundation:
    connectors:
        s3:
            default:
                endpoint: "http://localhost:9000"
                accessKeyId: "yourId"
                secretAccessKey: "yourPassword"
                forcePathStyle: true
                sslEnabled: false
```

## Development

### Tests

Run the file-assets tests

**Requirements:**

- `minio` - A running instance of minio. See this [Quickstart Guide](https://hub.docker.com/r/minio/minio).

```bash
yarn test
```

### Build

Build a compiled asset bundle to deploy to a teraslice cluster.

**Install Teraslice CLI**

```bash
yarn global add teraslice-cli
```

```bash
teraslice-cli assets build
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](./LICENSE) licensed.
