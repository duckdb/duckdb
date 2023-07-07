################################################################################
# R Package DuckDB extension config
################################################################################
#
# This is the default extension configuration for R builds. Basically it  means that all these extensions
# are "baked in" to the R binaries. Note that the configuration here is only when building R using the main
# CMakeLists.txt file with the `BUILD_R` variable.
# TODO: unify this by making setup.py also use this configuration, making this the config for all R builds
duckdb_extension_load(visualizer)
duckdb_extension_load(parquet)
duckdb_extension_load(icu)