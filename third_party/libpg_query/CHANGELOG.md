# Changelog

All versions are tagged by the major Postgres version, plus an individual semver for this library itself.

## 9.5-1.4.2    2016-12-06

* Cut off fingerprints at 100 nodes deep to avoid excessive runtimes/memory
* Fix warning on Linux due to missing asprintf include


## 9.5-1.4.1    2016-06-26

* Automatically call pg_query_init as needed to ease threaded usage


## 9.5-1.4.0    2016-06-26

* Clean up includes to avoid dependency on stdbool.h and xlocale.h
* Change PL/pgSQL input to be the full CREATE FUNCTION statement
  * This is necessary for parsing, since we need the argument and return types
* Fingerprinting Version 1.1
  * Only ignore ResTarget.name when parent field name is targetList *and*
    we have a SelectStmt as a parent node (fixes UpdateStmt fingerprinting)
* Normalize the password in ALTER ROLE ... PASSWORD '123' statements
* Make library thread-safe through thread-local storage [#13](https://github.com/lfittl/libpg_query/issues/13)


## 9.5-1.3.0    2016-05-31

* Extract source code using LLVM instead of manually compiling the right objects
  * This speeds up build times considerably since we don't download the Postgres
    source anymore, instead shipping a partial copy created as part of a release.
* Experimental support for parsing PL/pgSQL source code (output format subject to change)


## 9.5-1.2.1    2016-05-17

* Make sure we encode special characters correctly in JSON output ([@zhm](https://github.com/zhm) [#11](https://github.com/lfittl/libpg_query/pull/11))


## 9.5-1.2.0    2016-05-16

* Fix stack overflow when parsing CREATE FOREIGN TABLE ([#9](https://github.com/lfittl/libpg_query/issues/9))
* Update to PostgreSQL 9.5.3


## 9.5-1.1.0    2016-04-17

* Add pg_query_fingerprint() method that uniquely identifies SQL queries,
  whilst ignoring formatting and individual constant values
* Update to PostgreSQL 9.5.2


## 9.5-1.0.0    2016-03-06

* First release based on PostgreSQL 9.5.1
* Make JSON_OUTPUT_V2 the default and remove outfuncs patch
  * NOTE: This is a backwards incompatible change in the output parsetree format!


## 9.4-1.0.0    2016-03-06

* First tagged release based on PostgreSQL 9.4.5
