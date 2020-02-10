# partition_by_key

The `partition_by_key` will specify a path as a DataEntity attribute on each record based on the record's `_key` metadata attribute. This path will take precedence over the exporter configuration.

This will add the path partition as the metadata attribute `file:partition` on the DataEntity, and can then be accessed with something like `record.getMetadata('file:partition')`.

# Options

## `path`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any valid path string | `''` | N |

This is the base portion of the path. If not provided, the string created from the `_key` will be the root of the path.

# Example opConfig

A configuration like

```
{
    "_op": "partition_by_key",
    "path": "/data"
}
```

with a record like

```
> console.log(record.getKey())
'DaTaEnTiTyK3Y'
```

will add a `file:partition` attribute to records as DataEntity metadata with a value of:

```
/data/_key=DaTaEnTiTyK3Y/
```

---

If no configuration options are provided, the `file:partition` will be something like

```
_key=DaTaEnTiTyK3Y/
```
