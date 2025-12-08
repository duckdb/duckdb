#include "add_numbers.h"
#include "duckdb_extension.h"

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection, duckdb_extension_info info, duckdb_extension_access *access) {
	// Register a demo function
	RegisterAddNumbersFunction(connection);

#ifdef DUCKDB_EXTENSION_API_VERSION_UNSTABLE
	// Test using the unstable API
	duckdb_arrow result;
	auto api_result = duckdb_query_arrow(connection, "SELECT 1 as a", &result);

	if (api_result != duckdb_state::DuckDBSuccess) {
		access->set_error(info, "Arrow Query failed during initialization");
		return false;
	}

	duckdb_destroy_arrow(&result);
#endif

	// Return true to indicate succesful initialization
	return true;
}
