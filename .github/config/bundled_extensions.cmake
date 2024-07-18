#
# Default extension config for DuckDB releases.
#
# This is the extension config that is used to build / test DuckDB releases, e.g.:
#  - Which extensions are statically linked into DuckDB
#  - Which extensions are tested
#  - Which extensions can be autoloaded
#
# Distributions that run this config:
# - Windows (64bit and 32bit) # TODO: 32bit needs autoloading disabled
# - Linux (arm64 and 32bit)
# - OSX (universal binary)

#
## Extensions that are linked
#
duckdb_extension_load(icu)
duckdb_extension_load(inet)
duckdb_extension_load(tpch)
duckdb_extension_load(json)
duckdb_extension_load(fts)
duckdb_extension_load(parquet)
duckdb_extension_load(autocomplete)

#
## Extensions that are not linked, but we do want to test them as part of the release build
#
duckdb_extension_load(tpcds DONT_LINK)
