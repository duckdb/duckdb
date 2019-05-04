#include "main/database.hpp"

using namespace duckdb;
using namespace std;

DuckDB::DuckDB(const char *path, bool read_only)
    : storage(*this, path ? string(path) : string(), read_only), catalog(storage), transaction_manager(storage), read_only(read_only) {
	// initialize the database
	storage.Initialize();
}
