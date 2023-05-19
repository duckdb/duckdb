################################################################################
# NodeJS DuckDB extension config
################################################################################
#
# This is the default extension configuration for NodeJS builds. Basically it
# means that all these extensions are "baked in" to the NodeJS binaries
duckdb_extension_load(json)
duckdb_extension_load(icu)