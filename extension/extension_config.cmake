################################################################################
# DuckDB extension base config
################################################################################
#
# This is the base DuckDB extension configuration file. The extensions loaded here are included in every DuckDB build.
# Note that this file is checked into version control; if you want to specify which extensions to load for local
# development, create `extension/extension_config_local.cmake` and specify extensions there.
# The local file is also loaded by the DuckDB CMake build but ignored by version control.

# these extensions are loaded by default on every build as they are an essential part of DuckDB
duckdb_extension_load(core_functions)
duckdb_extension_load(parquet)
