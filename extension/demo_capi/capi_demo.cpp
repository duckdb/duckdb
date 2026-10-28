#include "add_numbers.h"
#include "duckdb_extension.h"

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection, duckdb_extension_info info, duckdb_extension_access *access) {
	// Register a demo function
	RegisterAddNumbersFunction(connection);

	duckdb_arrow result;
	auto api_result = duckdb_query_arrow(connection, "SELECT 1 as a", &result);

	if (api_result != duckdb_state::DuckDBSuccess) {
		access->set_error(info, "Arrow Query failed during initialization");
		return false;
	}

	duckdb_destroy_arrow(&result);

	// Return true to indicate successful initialization
	return true;
}
