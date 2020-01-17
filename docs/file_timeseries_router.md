# timeseries_path_partitioner

The `timeseries_path_partitioner` will specify a path as a DataEntity attribute on each record based on the specified date field. This path will take precedence over the exporter configuration.

This will add the path partition as the metadata attribute `file:partition` on the DataEntity, and can then be accessed with something like `record.getMetadata('file:partition')`.

# Options

## `base_path`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any valid path string | `''` | N |

This is the base portion of the path. If not provided, the string created from the date will be the root of the path.

## `prefix`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any valid path string | `''` | N |

Optional file prefix.

## `date_field`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| `String` | `date` | N |

Specifies the record field contains the timeseries date

## `type`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| `daily`, `monthly`, `yearly` | `daily` | N |

Specifies the type of timeseries data

# Example opConfig

A configuration like

```
{
    "_op": "timeseries_path_partitioner",
    "base_path": "/data",
    "prefix": "processed",
    "date_field": "accepted",
    "type": "daily"
}
```

will add a `file:partition` attribute to records as DataEntity metadata with a value of:

```
/data/2020.01.05/processed
```

---

If no configuration options are provided, the `file:partition` will be something like

```
2020.01.05/
```
