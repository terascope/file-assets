# partition_by_date

The `partition_by_date` will specify a path as a DataEntity attribute on each record based on the specified date field. This path will take precedence over the exporter configuration.

This will add the path partition as the metadata attribute `file:partition` on the DataEntity, and can then be accessed with something like `record.getMetadata('file:partition')`.

# Options

## `path`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| Any valid path string | `''` | N |

This is the base portion of the path. If not provided, the string created from the date will be the root of the path.

## `field`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| `String` | `date` | N |

Specifies the record field contains the timeseries date

## `resolution`

| Valid Options | Default | Required |
| ----------- | ------- | -------- |
| `daily`, `monthly`, `yearly` | `daily` | N |

Specifies the resolution of partitioning for the timeseries data

# Example opConfig

A configuration like

```
{
    "_op": "partition_by_date",
    "path": "/data",
    "field": "accepted",
    "resolution": "daily"
}
```

will add a `file:partition` attribute to records as DataEntity metadata with a value of:

```
/data/date_year=2020/date_month=01/date_day=05/
```

---

If no configuration options are provided, the `file:partition` will be something like

```
date_year=2020/date_month=01/date_day=05/
```
