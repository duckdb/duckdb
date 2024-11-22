#include "add_numbers.h"
#include "duckdb_extension.h"

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection, duckdb_extension_info info, duckdb_extension_access *access) {
	// Register a demo function
	RegisterAddNumbersFunction(connection);

	// Return true to indicate succesful initialization
	return true;
}
