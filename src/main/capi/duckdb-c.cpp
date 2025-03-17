#include "duckdb/main/capi/capi_internal.hpp"

using duckdb::Connection;
using duckdb::DatabaseWrapper;
using duckdb::DBConfig;
using duckdb::DBInstanceCacheWrapper;
using duckdb::DuckDB;
using duckdb::ErrorData;

duckdb_instance_cache duckdb_create_instance_cache() {
	auto wrapper = new DBInstanceCacheWrapper();
	wrapper->instance_cache = duckdb::make_uniq<duckdb::DBInstanceCache>();
	return reinterpret_cast<duckdb_instance_cache>(wrapper);
}

void duckdb_destroy_instance_cache(duckdb_instance_cache *instance_cache) {
	if (instance_cache && *instance_cache) {
		auto wrapper = reinterpret_cast<DBInstanceCacheWrapper *>(*instance_cache);
		delete wrapper;
		*instance_cache = nullptr;
	}
}

duckdb_state duckdb_open_internal(DBInstanceCacheWrapper *cache, const char *path, duckdb_database *out,
                                  duckdb_config config, char **out_error) {
	auto wrapper = new DatabaseWrapper();
	try {
		DBConfig default_config;
		default_config.SetOptionByName("duckdb_api", "capi");

		DBConfig *db_config = &default_config;
		DBConfig *user_config = reinterpret_cast<DBConfig *>(config);
		if (user_config) {
			db_config = user_config;
		}

		if (cache) {
			duckdb::string path_str;
			if (path) {
				path_str = path;
			}
			wrapper->database = cache->instance_cache->GetOrCreateInstance(path_str, *db_config, true);
		} else {
			wrapper->database = duckdb::make_shared_ptr<DuckDB>(path, db_config);
		}

	} catch (std::exception &ex) {
		if (out_error) {
			ErrorData parsed_error(ex);
			*out_error = strdup(parsed_error.Message().c_str());
		}
		delete wrapper;
		return DuckDBError;

	} catch (...) { // LCOV_EXCL_START
		if (out_error) {
			*out_error = strdup("Unknown error");
		}
		delete wrapper;
		return DuckDBError;
	} // LCOV_EXCL_STOP

	*out = reinterpret_cast<duckdb_database>(wrapper);
	return DuckDBSuccess;
}

duckdb_state duckdb_get_or_create_from_cache(duckdb_instance_cache instance_cache, const char *path,
                                             duckdb_database *out_database, duckdb_config config, char **out_error) {
	if (!instance_cache) {
		if (out_error) {
			*out_error = strdup("instance cache cannot be nullptr");
		}
		return DuckDBError;
	}
	auto cache = reinterpret_cast<DBInstanceCacheWrapper *>(instance_cache);
	return duckdb_open_internal(cache, path, out_database, config, out_error);
}

duckdb_state duckdb_open_ext(const char *path, duckdb_database *out, duckdb_config config, char **error) {
	return duckdb_open_internal(nullptr, path, out, config, error);
}

duckdb_state duckdb_open(const char *path, duckdb_database *out) {
	return duckdb_open_ext(path, out, nullptr, nullptr);
}

void duckdb_close(duckdb_database *database) {
	if (database && *database) {
		auto wrapper = reinterpret_cast<DatabaseWrapper *>(*database);
		delete wrapper;
		*database = nullptr;
	}
}

duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out) {
	if (!database || !out) {
		return DuckDBError;
	}

	auto wrapper = reinterpret_cast<DatabaseWrapper *>(database);
	Connection *connection;
	try {
		connection = new Connection(*wrapper->database);
	} catch (...) { // LCOV_EXCL_START
		return DuckDBError;
	} // LCOV_EXCL_STOP

	*out = reinterpret_cast<duckdb_connection>(connection);
	return DuckDBSuccess;
}

void duckdb_interrupt(duckdb_connection connection) {
	if (!connection) {
		return;
	}
	Connection *conn = reinterpret_cast<Connection *>(connection);
	conn->Interrupt();
}

duckdb_query_progress_type duckdb_query_progress(duckdb_connection connection) {
	duckdb_query_progress_type query_progress_type;
	query_progress_type.percentage = -1;
	query_progress_type.total_rows_to_process = 0;
	query_progress_type.rows_processed = 0;
	if (!connection) {
		return query_progress_type;
	}
	Connection *conn = reinterpret_cast<Connection *>(connection);
	auto query_progress = conn->context->GetQueryProgress();
	query_progress_type.total_rows_to_process = query_progress.GetTotalRowsToProcess();
	query_progress_type.rows_processed = query_progress.GetRowsProcesseed();
	query_progress_type.percentage = query_progress.GetPercentage();
	return query_progress_type;
}

void duckdb_disconnect(duckdb_connection *connection) {
	if (connection && *connection) {
		Connection *conn = reinterpret_cast<Connection *>(*connection);
		delete conn;
		*connection = nullptr;
	}
}

duckdb_state duckdb_query(duckdb_connection connection, const char *query, duckdb_result *out) {
	Connection *conn = reinterpret_cast<Connection *>(connection);
	auto result = conn->Query(query);
	return DuckDBTranslateResult(std::move(result), out);
}

const char *duckdb_library_version() {
	return DuckDB::LibraryVersion();
}
