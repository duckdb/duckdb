#define DUCKB_NO_CAPI_FUNCTIONS
#include "duckdb_extension.h"
#include "add_numbers.h"

DUCKDB_EXTENSION_MAIN

//===--------------------------------------------------------------------===//
// Scalar function 1
//===--------------------------------------------------------------------===//

static void RegisterAdditionFunction(duckdb_connection connection) {
	// create a scalar function
	auto function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(function, "add_numbers_together");

	// add a two bigint parameters
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	duckdb_scalar_function_add_parameter(function, type);
	duckdb_scalar_function_add_parameter(function, type);

	// set the return type to bigint
	duckdb_scalar_function_set_return_type(function, type);

	duckdb_destroy_logical_type(&type);

	// set up the function
	duckdb_scalar_function_set_function(function, AddNumbersTogether);

	// register and cleanup
	duckdb_register_scalar_function(connection, function);

	duckdb_destroy_scalar_function(&function);
}

//===--------------------------------------------------------------------===//
// Extension load + setup
//===--------------------------------------------------------------------===//
extern "C" {

DUCKDB_EXTENSION_API void demo_capi_init_capi(duckdb_connection &db, duckdb_ext_api_v0* api) {
	DUCKDB_EXTENSION_LOAD_API(api);

	RegisterAdditionFunction(db);
}

//! the version_capi version returns the version of the CAPI they require to run
// Simple versioning scheme would be: v1.x.x for the duckdb_ext_api_v1_v1 struct,
//							          v2.x.x for the duckdb_ext_api_v1_v2 struct etc.
DUCKDB_EXTENSION_API const char * demo_capi_capi_version(duckdb_connection &db) {
	return "v0.0.1";
}

// NOTE: CAPI extension will not implement this one
// DUCKDB_EXTENSION_API void loadable_extension_demo_c_api_init_capi(DatabaseInstance &db) {
// }

// NOTE: CAPI extensions will return "" here to indicate that they are not tied to a specific DuckDB
// version.
DUCKDB_EXTENSION_API const char *demo_capi_version() {
	return "";
}
}
