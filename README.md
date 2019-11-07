# CAUTION UNDER DEVELOPMENT
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
- **binlog_filename**: MySQL binlog filename (string, required)
- **binlog_position**: MySQL binlog postion (integer, required)
- **enable_metadata**: flag to add metadata to each row (bool, default: `true`)
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
  binlog_filename: mysql-binlog.00001
  binlog_position: 4
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
