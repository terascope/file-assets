# partition_by_fields

The `partition_by_fields` will specify a path as a DataEntity attribute on each record based on the specified fields. This path will take precedence over the exporter configuration.

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

Specifies the fields to include in the partitioning scheme. At least one field must be specified, and field order is important.

# Example opConfig

A configuration like

```
{
    "_op": "partition_by_fields",
    "path": "/data",
    "fields": [
        "field2",
        "field1"
    ]
}
```

and a record like

```
{
    "field1": "val1",
    "field2": "val2"
}
```

will add a `file:partition` attribute to the record as DataEntity metadata with a value of:

```
/data/field2=val2/field1=val1/
```

---

If no path is provided, the `file:partition` will be something like

```
field2=val2/field1=val1/
```
