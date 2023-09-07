#define DUCKDB_EXTENSION_MAIN

#include "duckdb/main/extension_util.hpp"

extern "C" {

DUCKDB_EXTENSION_API const char *error_message() { // NOLINT
	return "This is a placeholder DuckDB extension";
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
