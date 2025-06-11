# file-assets

> A set of Teraslice processors for working with data stored in files on disk. The readers utilize the `chunked-file-reader` module (migrated into this bundle from the Teraslice monorepo) to break data into records.

Since all the readers in this asset bundle use DataEntities, the slice's file path can be retrieved from each record by using something like `record.getMetadata('path')`. More information about DataEntities can be found [here](https://terascope.github.io/teraslice/docs/packages/utils/api/entities/data-entity/classes/DataEntity).

This bundle includes the following processors:

- [`file_exporter`](./docs/asset/operations/file_exporter.md)
- [`file_reader`](./docs/asset/operations/file_reader.md)
- [`s3_exporter`](./docs/asset/operations/s3_exporter.md)
- [`s3_reader`](./docs/asset/operations/s3_reader.md)
- [`file_sender_api`](./docs/asset/apis/file_sender_api.md)
- [`file_reader_api`](./docs/asset/apis/file_reader_api.md)
- [`s3_sender_api`](./docs/asset/apis/s3_sender_api.md)
- [`s3_reader_api`](./docs/asset/apis/s3_reader_api.md)

## Releases

You can find a list of releases, changes, and pre-built asset bundles [here](https://github.com/terascope/file-assets/releases).

## Getting Started

This asset bundle requires a running Teraslice cluster [Documentation](https://github.com/terascope/teraslice/blob/master/README.md).

```bash
# Step 1: make sure you have teraslice-cli installed
yarn global add teraslice-cli

# Step 2:
teraslice-cli assets deploy clusterAlias terascope/file-assets
```

## Connectors

### S3 Connector

**Configuration:**

The S3 connector configuration, in your Teraslice configuration file, includes the following parameters:

| Configuration | Description | Type |  Notes |
| --------- | -------- | ------ | ------ |
| endpoint | Target S3 HTTP endpoint, must be URL | String | optional, defaults to `http://127.0.0.1:80` |
| accessKeyId | S3 access key ID | String | required |
| secretAccessKey | S3 secret access key | String | required |
| region | AWS Region where bucket is located | String | optional, defaults to `us-east-1` |
| maxRetries | Maximum retry attempts | Number | optional, defaults to `3` |
| sslEnabled | Flag to enable/disable SSL communication | Boolean | optional, defaults to `true` |
| caCertificate | A string containing a single or multiple ca certificates | String | optional, defaults to ' ' |
| certLocation | DEPRECATED - use caCertificate. Location of ssl cert | String | optional, defaults to ' ' |
| forcePathStyle | Whether to force path style URLs for S3 objects | Boolean | optional, defaults to `false` |
| bucketEndpoint | Whether to use the bucket name as the endpoint for this request | Boolean | optional, defaults to `false` |

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
                sslEnabled: true
                caCertificate: |
                    -----BEGIN CERTIFICATE-----
                    MIICGTCCAZ+gAwIBAgIQCeCTZaz32ci5PhwLBCou8zAKBggqhkjOPQQDAzBOMQs
                    ...
                    DXZDjC5Ty3zfDBeWUA==
                    -----END CERTIFICATE-----
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
