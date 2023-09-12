################################################################################
# NodeJS DuckDB extension config
################################################################################
#
# This is the default extension configuration for NodeJS builds. Basically it means that all these extensions are
# "baked in" to the NodeJS binaries Note that the configuration here is only when building Node using the main
# CMakeLists.txt file with the `BUILD_NODE` variable.
# TODO: unify this by making setup.py also use this configuration, making this the config for all Node builds
duckdb_extension_load(json)
duckdb_extension_load(icu)