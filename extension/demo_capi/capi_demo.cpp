#include "add_numbers.h"
#include "duckdb_extension.h"

DUCKDB_EXTENSION_GLOBAL

static void Entrypoint(duckdb_connection connection, duckdb_extension_info info, duckdb_extension_access *access) {
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

extern "C" {
DUCKDB_EXTENSION_REGISTER_ENTRYPOINT(demo_capi, Entrypoint, CAPI_VERSION)
}
