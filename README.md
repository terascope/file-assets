# file-assets
> A set of Teraslice processors for working with data stored in files on disk. The readers utilize the [chunked-file-reader](https://terascope.github.io/teraslice/docs/packages/chunked-file-reader/overview) module, which is shared across the file-related asset bundles.

Since all the readers in this asset bundle use DataEntities, the slice's file path can be retrieved from each record by using something like `record.getMetadata('path')`. More information about DataEntities can be found [here](https://terascope.github.io/teraslice/docs/packages/utils/api/classes/dataentity).

This bundle includes the following processors:
- [`file_exporter`](./docs/file_exporter.md)
- [`file_reader`](./docs/file_reader.md)
- `compressed_file_reader`

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](./LICENSE) licensed.
