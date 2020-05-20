# Mysql Binlog input plugin for Embulk

**This plugin is under development**.

MySQL input plugin for Embulk loads data by binlog.


## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration

- **host**: MySQL host (string, required)
- **port**: MySQL port (integer, default: `3306`)
- **database**: MySQL database (string, required)
- **user**: MySQL user (string, required)
- **table**: MySQL user (string, required)
- **password**: MySQL password (string, required)
- **from_binlog_filename**: The beginning of MySQL binlog filename (string, required)
- **from_binlog_position**: The beginning of MySQL binlog position (integer, required)
- **to_binlog_filename**: The end of MySQL binlog filename (string, optional) if to_binlog_filename is provided and to_binlog_position is omitted, this plugin stop fetching date just after binlog rotation to this file. if to_binlog_filename is omitted, plugin stops at the end of binlog.
- **to_binlog_position**: The end of MySQL binlog position (integer, optional) if to_binlog_filename is omitted, plugin stops at the end of binlog.
- **enable_metadata_deleted**: flag to add metadata deleted to each row (bool, default: `true`)
- **enable_metadata_fetched_at**: flag to add metadata synced_at to each row (bool, default: `true`)
- **enable_metadata_seq**: sequence number of record (bool, default: `true`)
- **metadata_prefix**: metadata prefix (string, default: `_`)
- **columns**: MySQL column
    - name: name of the column
    - type: data type of the column
    - format: timestamp format

## Example

```yaml
in:
  type: mysql_binlog
  host: localhost 
  port: 3306
  database: test
  table: test
  user: username
  password: password
  from_binlog_filename: mysql-binlog.00001
  from_binlog_position: 4
  enable_metadata: true
  metadata_prefix: _trocco
  columns:
    - {name: id, type: long}
    - {name: name, type: string}
  
```

### Supported MySQL version

MySQL > 5.6.2, due to crush safe

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
