# partition_by_hash

The `partition_by_hash` will specify a path as a DataEntity attribute on each record based on the hash of the specified fields. The fields will be concatenated in the order specified in the configuration and partitioned by the number of specified partitions. This path will take precedence over the exporter configuration.

This will add the path partition as the metadata attribute `file:partition` on the DataEntity, and can then be accessed with something like `record.getMetadata('file:partition')`.

# Options

## `path`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any valid path string | `''` | N |

This is the base portion of the path. If not provided, the string created from the date will be the root of the path.

## `fields`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| An array of fields | `[]` | Y |

Specifies the fields to use for the hash. At least one field must be specified, and field order is important.

## `partitions`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Number | `null` | Y |

Specifies the number of partitions to hash records into.

# Example opConfig

A configuration like

```
{
    "_op": "partition_by_hash",
    "path": "/data",
    "fields": [
        "field2",
        "field1"
    ],
    "partitions": 10
}
```

will add a `file:partition` attribute to the records as DataEntity metadata with a value like:

```
/data/partition=5/
```

The partition number will change depending on the values of the specified fields and will always be between `0` and `${partitions} - 1` inclusive.

---

If no path is provided, the `file:partition` will be something like

```
partition=5/
```
