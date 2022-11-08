#include "duckdb/main/capi/capi_internal.hpp"

using duckdb::Connection;
using duckdb::DatabaseData;
using duckdb::DBConfig;
using duckdb::DuckDB;

duckdb_state duckdb_open_ext(const char *path, duckdb_database *out, duckdb_config config, char **error) {
	auto wrapper = new DatabaseData();
	try {
		auto db_config = (DBConfig *)config;
		wrapper->database = duckdb::make_unique<DuckDB>(path, db_config);
	} catch (std::exception &ex) {
		if (error) {
			*error = strdup(ex.what());
		}
		delete wrapper;
		return DuckDBError;
	} catch (...) { // LCOV_EXCL_START
		if (error) {
			*error = strdup("Unknown error");
		}
		delete wrapper;
		return DuckDBError;
	} // LCOV_EXCL_STOP
	*out = (duckdb_database)wrapper;
	return DuckDBSuccess;
}

duckdb_state duckdb_open(const char *path, duckdb_database *out) {
	return duckdb_open_ext(path, out, nullptr, nullptr);
}

void duckdb_close(duckdb_database *database) {
	if (database && *database) {
		auto wrapper = (DatabaseData *)*database;
		delete wrapper;
		*database = nullptr;
	}
}

duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out) {
	if (!database || !out) {
		return DuckDBError;
	}
	auto wrapper = (DatabaseData *)database;
	Connection *connection;
	try {
		connection = new Connection(*wrapper->database);
	} catch (...) { // LCOV_EXCL_START
		return DuckDBError;
	} // LCOV_EXCL_STOP
	*out = (duckdb_connection)connection;
	return DuckDBSuccess;
}

void duckdb_disconnect(duckdb_connection *connection) {
	if (connection && *connection) {
		Connection *conn = (Connection *)*connection;
		delete conn;
		*connection = nullptr;
	}
}

duckdb_state duckdb_query(duckdb_connection connection, const char *query, duckdb_result *out) {
	Connection *conn = (Connection *)connection;
	auto result = conn->Query(query);
	return duckdb_translate_result(move(result), out);
}

const char *duckdb_library_version() {
	return DuckDB::LibraryVersion();
}
