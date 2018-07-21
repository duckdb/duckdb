
#include "duckdb.h"
#include "duckdb.hpp"

using namespace duckdb;

duckdb_state duckdb_open(char *path, duckdb_database *out) {
	DuckDB *database = new DuckDB(path);
	*out = (duckdb_database)database;
	return DuckDBSuccess;
}

duckdb_state duckdb_close(duckdb_database database) {
	if (database) {
		DuckDB *db = (DuckDB *)database;
		delete db;
	}
	return DuckDBSuccess;
}

duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out) {
	DuckDB *db = (DuckDB *)database;
	DuckDBConnection *connection = new DuckDBConnection(*db);
	*out = (duckdb_connection)connection;
	return DuckDBSuccess;
}

duckdb_state duckdb_disconnect(duckdb_connection connection) {
	if (connection) {
		DuckDBConnection *conn = (DuckDBConnection *)connection;
		delete conn;
	}
	return DuckDBSuccess;
}

duckdb_state duckdb_query(duckdb_connection connection, const char *query,
                          duckdb_result *out) {
	DuckDBConnection *conn = (DuckDBConnection *)connection;
	auto result = conn->Query(query);
	if (!result->GetSuccess()) {
		result->Print();
		return DuckDBError;
	}
	result->Print();
	*out = nullptr;
	return DuckDBSuccess;
}
