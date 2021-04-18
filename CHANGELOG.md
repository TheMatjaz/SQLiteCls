Changelog
===============================================================================

All notable changes to this project will be documented in this file.

The format is based on 
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to 
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).


[0.1.0] - 2021-04-18
----------------------------------------

Initial version


### Added

- SQLiteDb class wrapping the operations from the `sqlite3` module,
  providing better support for the `with` operator and automatic
  initialisation of an empty DB upon first creation of the DB.
- Additional wrappers are available as a utility, including
  extraction of a list of tables, extraction of the list of columns
  of a SELECT query, commit, rollback, start of a transaction,
  check if the DB is in memory and if it's empty. Executions of SQL script
  files is also made easy.
