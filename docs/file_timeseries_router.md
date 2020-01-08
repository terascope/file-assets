# file_timeseries_router

The `file_timeseries_router` will specify a path as a DataEntity attribute on each record based on the specified date field. This path will take precedence over the exporter configuration.

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
    "_op": "file_timeseries_router",
    "base_path": "/data",
    "prefix": "processed_",
    "date_field": "accepted",
    "type": "daily"
}
```

will add a `routingPath` attribute to records as DataEntity metadata and would result in values something like

```
/data/2020.01.05/processed_
```

---

If no configuration options are provided, the `routingPath` will be something like

```
2020.01.05/
```
