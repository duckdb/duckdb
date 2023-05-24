#include "duckdb/common/adbc/adbc.hpp"
#include "duckdb/common/adbc/adbc-init.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"

#include "duckdb.h"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow.hpp"

#include <string.h>
#include <stdlib.h>

// We must leak the symbols of the init function
duckdb_adbc::AdbcStatusCode duckdb_adbc_init(size_t count, struct duckdb_adbc::AdbcDriver *driver,
                                             struct duckdb_adbc::AdbcError *error) {
	if (!driver) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}

	driver->DatabaseNew = duckdb_adbc::DatabaseNew;
	driver->DatabaseSetOption = duckdb_adbc::DatabaseSetOption;
	driver->DatabaseInit = duckdb_adbc::DatabaseInit;
	driver->DatabaseRelease = duckdb_adbc::DatabaseRelease;
	driver->ConnectionNew = duckdb_adbc::ConnectionNew;
	driver->ConnectionSetOption = duckdb_adbc::ConnectionSetOption;
	driver->ConnectionInit = duckdb_adbc::ConnectionInit;
	driver->ConnectionRelease = duckdb_adbc::ConnectionRelease;
	driver->ConnectionGetTableTypes = duckdb_adbc::ConnectionGetTableTypes;
	driver->StatementNew = duckdb_adbc::StatementNew;
	driver->StatementRelease = duckdb_adbc::StatementRelease;
	//	driver->StatementBind = duckdb::adbc::StatementBind;
	driver->StatementBindStream = duckdb_adbc::StatementBindStream;
	driver->StatementExecuteQuery = duckdb_adbc::StatementExecuteQuery;
	driver->StatementPrepare = duckdb_adbc::StatementPrepare;
	driver->StatementSetOption = duckdb_adbc::StatementSetOption;
	driver->StatementSetSqlQuery = duckdb_adbc::StatementSetSqlQuery;
	driver->ConnectionGetObjects = duckdb_adbc::ConnectionGetObjects;
	return ADBC_STATUS_OK;
}

namespace duckdb_adbc {
AdbcStatusCode SetErrorMaybe(const void *result, AdbcError *error, const std::string &error_message) {
	if (!error) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!result) {
		SetError(error, error_message);
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	return ADBC_STATUS_OK;
}

struct DuckDBAdbcDatabaseWrapper {
	//! The DuckDB Database Configuration
	::duckdb_config config;
	//! The DuckDB Database
	::duckdb_database database;
	//! Path of Disk-Based Database or :memory: database
	std::string path;
};

void InitiliazeADBCError(AdbcError *error) {
	if (!error) {
		return;
	}
	error->message = nullptr;
	error->release = nullptr;
	std::memset(error->sqlstate, '\0', sizeof(error->sqlstate));
	error->vendor_code = -1;
}

AdbcStatusCode CheckResult(duckdb_state &res, AdbcError *error, const char *error_msg) {
	if (!error) {
		// Error should be a non-null pointer
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (res != DuckDBSuccess) {
		duckdb_adbc::SetError(error, error_msg);
		return ADBC_STATUS_INTERNAL;
	}
	return ADBC_STATUS_OK;
}

AdbcStatusCode DatabaseNew(struct AdbcDatabase *database, struct AdbcError *error) {
	auto status = SetErrorMaybe(database, error, "Missing database object");
	if (status != ADBC_STATUS_OK) {
		return status;
	}
	database->private_data = nullptr;
	// you can't malloc a struct with a non-trivial C++ constructor
	// and std::string has a non-trivial constructor. so we need
	// to use new and delete rather than malloc and free.
	auto wrapper = new DuckDBAdbcDatabaseWrapper;
	status = SetErrorMaybe(wrapper, error, "Allocation error");
	if (status != ADBC_STATUS_OK) {
		return status;
	}
	database->private_data = wrapper;
	auto res = duckdb_create_config(&wrapper->config);
	return CheckResult(res, error, "Failed to allocate");
}

AdbcStatusCode DatabaseSetOption(struct AdbcDatabase *database, const char *key, const char *value,
                                 struct AdbcError *error) {
	auto status = SetErrorMaybe(database, error, "Missing database object");
	if (status != ADBC_STATUS_OK) {
		return status;
	}

	status = SetErrorMaybe(key, error, "Missing key");
	if (status != ADBC_STATUS_OK) {
		return status;
	}

	auto wrapper = (DuckDBAdbcDatabaseWrapper *)database->private_data;
	if (strcmp(key, "path") == 0) {
		wrapper->path = value;
		return ADBC_STATUS_OK;
	}
	auto res = duckdb_set_config(wrapper->config, key, value);

	return CheckResult(res, error, "Failed to set configuration option");
}

AdbcStatusCode DatabaseInit(struct AdbcDatabase *database, struct AdbcError *error) {
	if (!error) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!database) {
		duckdb_adbc::SetError(error, "ADBC Database has an invalid pointer");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	char *errormsg;
	// TODO can we set the database path via option, too? Does not look like it...
	auto wrapper = (DuckDBAdbcDatabaseWrapper *)database->private_data;
	auto res = duckdb_open_ext(wrapper->path.c_str(), &wrapper->database, wrapper->config, &errormsg);
	return CheckResult(res, error, errormsg);
}

AdbcStatusCode DatabaseRelease(struct AdbcDatabase *database, struct AdbcError *error) {

	if (database && database->private_data) {
		auto wrapper = (DuckDBAdbcDatabaseWrapper *)database->private_data;

		duckdb_close(&wrapper->database);
		duckdb_destroy_config(&wrapper->config);
		delete wrapper;
		database->private_data = nullptr;
	}
	return ADBC_STATUS_OK;
}

AdbcStatusCode ConnectionNew(struct AdbcConnection *connection, struct AdbcError *error) {
	auto status = SetErrorMaybe(connection, error, "Missing connection object");
	if (status != ADBC_STATUS_OK) {
		return status;
	}

	connection->private_data = nullptr;
	return ADBC_STATUS_OK;
}

AdbcStatusCode ConnectionSetOption(struct AdbcConnection *connection, const char *key, const char *value,
                                   struct AdbcError *error) {
	// there are no connection-level options that need to be set before connecting
	return ADBC_STATUS_OK;
}

AdbcStatusCode ConnectionInit(struct AdbcConnection *connection, struct AdbcDatabase *database,
                              struct AdbcError *error) {
	auto status = SetErrorMaybe(database, error, "Missing database");
	if (status != ADBC_STATUS_OK) {
		return status;
	}
	status = SetErrorMaybe(database->private_data, error, "Invalid database");
	if (status != ADBC_STATUS_OK) {
		return status;
	}
	status = SetErrorMaybe(connection, error, "Missing connection");
	if (status != ADBC_STATUS_OK) {
		return status;
	}
	auto database_wrapper = (DuckDBAdbcDatabaseWrapper *)database->private_data;

	connection->private_data = nullptr;
	auto res = duckdb_connect(database_wrapper->database, (duckdb_connection *)&connection->private_data);
	return CheckResult(res, error, "Failed to connect to Database");
}

AdbcStatusCode ConnectionRelease(struct AdbcConnection *connection, struct AdbcError *error) {
	if (connection && connection->private_data) {
		duckdb_disconnect((duckdb_connection *)&connection->private_data);
		connection->private_data = nullptr;
	}
	return ADBC_STATUS_OK;
}

// some stream callbacks

static int get_schema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
	if (!stream || !stream->private_data || !out) {
		return DuckDBError;
	}
	return duckdb_query_arrow_schema((duckdb_arrow)stream->private_data, (duckdb_arrow_schema *)&out);
}

static int get_next(struct ArrowArrayStream *stream, struct ArrowArray *out) {
	if (!stream || !stream->private_data || !out) {
		return DuckDBError;
	}
	out->release = nullptr;

	return duckdb_query_arrow_array((duckdb_arrow)stream->private_data, (duckdb_arrow_array *)&out);
}

void release(struct ArrowArrayStream *stream) {
	if (!stream || !stream->release) {
		return;
	}
	stream->release = nullptr;
	if (stream->private_data) {
		duckdb_destroy_arrow((duckdb_arrow *)&stream->private_data);
		stream->private_data = nullptr;
	}
}

const char *get_last_error(struct ArrowArrayStream *stream) {
	if (!stream) {
		return nullptr;
	}
	return nullptr;
	// return duckdb_query_arrow_error(stream);
}

// this is an evil hack, normally we would need a stream factory here, but its probably much easier if the adbc clients
// just hand over a stream

duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper>
stream_produce(uintptr_t factory_ptr,
               std::pair<std::unordered_map<idx_t, std::string>, std::vector<std::string>> &project_columns,
               duckdb::TableFilterSet *filters) {

	// TODO this will ignore any projections or filters but since we don't expose the scan it should be sort of fine
	auto res = duckdb::make_uniq<duckdb::ArrowArrayStreamWrapper>();
	res->arrow_array_stream = *(ArrowArrayStream *)factory_ptr;
	return res;
}

void stream_schema(uintptr_t factory_ptr, duckdb::ArrowSchemaWrapper &schema) {
	auto stream = (ArrowArrayStream *)factory_ptr;
	get_schema(stream, &schema.arrow_schema);
}

AdbcStatusCode Ingest(duckdb_connection connection, const char *table_name, struct ArrowArrayStream *input,
                      struct AdbcError *error) {

	auto status = SetErrorMaybe(connection, error, "Invalid connection");
	if (status != ADBC_STATUS_OK) {
		return status;
	}

	status = SetErrorMaybe(input, error, "Missing input arrow stream pointer");
	if (status != ADBC_STATUS_OK) {
		return status;
	}

	status = SetErrorMaybe(table_name, error, "Missing database object name");
	if (status != ADBC_STATUS_OK) {
		return status;
	}

	try {
		// TODO evil cast, do we need a way to do this from the C api?
		auto cconn = (duckdb::Connection *)connection;
		cconn
		    ->TableFunction("arrow_scan",
		                    {duckdb::Value::POINTER((uintptr_t)input),
		                     duckdb::Value::POINTER((uintptr_t)stream_produce),
		                     duckdb::Value::POINTER((uintptr_t)get_schema)}) // TODO make this a parameter somewhere
		    ->Create(table_name); // TODO this should probably be a temp table
		// After creating a table, the arrow array stream is released. Hence we must set it as released to avoid
		// double-releasing it
		input->release = nullptr;
	} catch (std::exception &ex) {
		if (error) {
			error->message = strdup(ex.what());
		}
		return ADBC_STATUS_INTERNAL;
	} catch (...) {
		return ADBC_STATUS_INTERNAL;
	}
	return ADBC_STATUS_OK;
}

struct DuckDBAdbcStatementWrapper {
	::duckdb_connection connection;
	::duckdb_arrow result;
	::duckdb_prepared_statement statement;
	char *ingestion_table_name;
	ArrowArrayStream *ingestion_stream;
};

AdbcStatusCode StatementNew(struct AdbcConnection *connection, struct AdbcStatement *statement,
                            struct AdbcError *error) {

	auto status = SetErrorMaybe(connection, error, "Missing connection object");
	if (status != ADBC_STATUS_OK) {
		return status;
	}

	status = SetErrorMaybe(connection->private_data, error, "Invalid connection object");
	if (status != ADBC_STATUS_OK) {
		return status;
	}

	status = SetErrorMaybe(statement, error, "Missing statement object");
	if (status != ADBC_STATUS_OK) {
		return status;
	}

	statement->private_data = nullptr;

	auto statement_wrapper = (DuckDBAdbcStatementWrapper *)malloc(sizeof(DuckDBAdbcStatementWrapper));
	status = SetErrorMaybe(statement_wrapper, error, "Allocation error");
	if (status != ADBC_STATUS_OK) {
		return status;
	}

	statement->private_data = statement_wrapper;
	statement_wrapper->connection = (duckdb_connection)connection->private_data;
	statement_wrapper->statement = nullptr;
	statement_wrapper->result = nullptr;
	statement_wrapper->ingestion_stream = nullptr;
	statement_wrapper->ingestion_table_name = nullptr;
	return ADBC_STATUS_OK;
}

AdbcStatusCode StatementRelease(struct AdbcStatement *statement, struct AdbcError *error) {

	if (statement && statement->private_data) {
		auto wrapper = (DuckDBAdbcStatementWrapper *)statement->private_data;
		if (wrapper->statement) {
			duckdb_destroy_prepare(&wrapper->statement);
			wrapper->statement = nullptr;
		}
		if (wrapper->result) {
			duckdb_destroy_arrow(&wrapper->result);
			wrapper->result = nullptr;
		}
		if (wrapper->ingestion_stream) {
			wrapper->ingestion_stream->release(wrapper->ingestion_stream);
			wrapper->ingestion_stream->release = nullptr;
			wrapper->ingestion_stream = nullptr;
		}
		if (wrapper->ingestion_table_name) {
			free(wrapper->ingestion_table_name);
			wrapper->ingestion_table_name = nullptr;
		}
		free(statement->private_data);
		statement->private_data = nullptr;
	}
	return ADBC_STATUS_OK;
}

AdbcStatusCode StatementExecuteQuery(struct AdbcStatement *statement, struct ArrowArrayStream *out,
                                     int64_t *rows_affected, struct AdbcError *error) {
	auto status = SetErrorMaybe(statement, error, "Missing statement object");
	if (status != ADBC_STATUS_OK) {
		return status;
	}

	status = SetErrorMaybe(statement->private_data, error, "Invalid statement object");
	if (status != ADBC_STATUS_OK) {
		return status;
	}

	auto wrapper = (DuckDBAdbcStatementWrapper *)statement->private_data;

	// TODO: Set affected rows, careful with early return
	if (rows_affected) {
		*rows_affected = 0;
	}

	if (wrapper->ingestion_stream && wrapper->ingestion_table_name) {
		auto stream = wrapper->ingestion_stream;
		wrapper->ingestion_stream = nullptr;
		return Ingest(wrapper->connection, wrapper->ingestion_table_name, stream, error);
	}

	auto res = duckdb_execute_prepared_arrow(wrapper->statement, &wrapper->result);
	if (res != DuckDBSuccess) {
		SetError(error, duckdb_query_arrow_error(wrapper->result));
		return ADBC_STATUS_INVALID_ARGUMENT;
	}

	if (out) {
		out->private_data = wrapper->result;
		out->get_schema = get_schema;
		out->get_next = get_next;
		out->release = release;
		out->get_last_error = get_last_error;

		// because we handed out the stream pointer its no longer our responsibility to destroy it in
		// AdbcStatementRelease, this is now done in release()
		wrapper->result = nullptr;
	}

	return ADBC_STATUS_OK;
}

// this is a nop for us
AdbcStatusCode StatementPrepare(struct AdbcStatement *statement, struct AdbcError *error) {
	auto status = SetErrorMaybe(statement, error, "Missing statement object");
	if (status != ADBC_STATUS_OK) {
		return status;
	}

	status = SetErrorMaybe(statement->private_data, error, "Invalid statement object");
	if (status != ADBC_STATUS_OK) {
		return status;
	}

	return ADBC_STATUS_OK;
}

AdbcStatusCode StatementSetSqlQuery(struct AdbcStatement *statement, const char *query, struct AdbcError *error) {
	auto status = SetErrorMaybe(statement, error, "Missing statement object");
	if (status != ADBC_STATUS_OK) {
		return status;
	}
	status = SetErrorMaybe(query, error, "Missing query");
	if (status != ADBC_STATUS_OK) {
		return status;
	}

	auto wrapper = (DuckDBAdbcStatementWrapper *)statement->private_data;
	auto res = duckdb_prepare(wrapper->connection, query, &wrapper->statement);
	auto error_msg = duckdb_prepare_error(wrapper->statement);
	return CheckResult(res, error, error_msg);
}

AdbcStatusCode StatementBindStream(struct AdbcStatement *statement, struct ArrowArrayStream *values,
                                   struct AdbcError *error) {
	auto status = SetErrorMaybe(statement, error, "Missing statement object");
	if (status != ADBC_STATUS_OK) {
		return status;
	}
	status = SetErrorMaybe(values, error, "Missing stream object");
	if (status != ADBC_STATUS_OK) {
		return status;
	}
	auto wrapper = (DuckDBAdbcStatementWrapper *)statement->private_data;
	wrapper->ingestion_stream = values;
	return ADBC_STATUS_OK;
}

AdbcStatusCode StatementSetOption(struct AdbcStatement *statement, const char *key, const char *value,
                                  struct AdbcError *error) {
	auto status = SetErrorMaybe(statement, error, "Missing statement object");
	if (status != ADBC_STATUS_OK) {
		return status;
	}
	status = SetErrorMaybe(key, error, "Missing key object");
	if (status != ADBC_STATUS_OK) {
		return status;
	}
	auto wrapper = (DuckDBAdbcStatementWrapper *)statement->private_data;

	if (strcmp(key, ADBC_INGEST_OPTION_TARGET_TABLE) == 0) {
		wrapper->ingestion_table_name = strdup(value);
		return ADBC_STATUS_OK;
	}
	return ADBC_STATUS_INVALID_ARGUMENT;
}

static AdbcStatusCode QueryInternal(struct AdbcConnection *connection, struct ArrowArrayStream *out, const char *query,
                                    struct AdbcError *error) {
	AdbcStatement statement;

	auto status = StatementNew(connection, &statement, error);
	if (status != ADBC_STATUS_OK) {
		SetError(error, "unable to initialize statement");
		return status;
	}
	status = StatementSetSqlQuery(&statement, query, error);
	if (status != ADBC_STATUS_OK) {
		SetError(error, "unable to initialize statement");
		return status;
	}
	status = StatementExecuteQuery(&statement, out, nullptr, error);
	if (status != ADBC_STATUS_OK) {
		SetError(error, "unable to initialize statement");
		return status;
	}

	return ADBC_STATUS_OK;
}

AdbcStatusCode ConnectionGetObjects(struct AdbcConnection *connection, int depth, const char *catalog,
                                    const char *db_schema, const char *table_name, const char **table_type,
                                    const char *column_name, struct ArrowArrayStream *out, struct AdbcError *error) {
	if (catalog != nullptr) {
		if (strcmp(catalog, "duckdb") == 0) {
			SetError(error, "catalog must be NULL or 'duckdb'");
			return ADBC_STATUS_INVALID_ARGUMENT;
		}
	}

	if (table_type != nullptr) {
		SetError(error, "Table types parameter not yet supported");
		return ADBC_STATUS_NOT_IMPLEMENTED;
	}

	auto q = duckdb::StringUtil::Format(R"(
SELECT table_schema db_schema_name, LIST(table_schema_list) db_schema_tables FROM (
	SELECT table_schema, { table_name : table_name, table_columns : LIST({column_name : column_name, ordinal_position : ordinal_position + 1, remarks : ''})} table_schema_list FROM information_schema.columns WHERE table_schema LIKE '%s' AND table_name LIKE '%s' AND column_name LIKE '%s' GROUP BY table_schema, table_name
	) GROUP BY table_schema;
)",
	                                    db_schema ? db_schema : "%", table_name ? table_name : "%",
	                                    column_name ? column_name : "%");

	return QueryInternal(connection, out, q.c_str(), error);
}

AdbcStatusCode ConnectionGetTableTypes(struct AdbcConnection *connection, struct ArrowArrayStream *out,
                                       struct AdbcError *error) {
	const char *q = "SELECT DISTINCT table_type FROM information_schema.tables ORDER BY table_type";
	return QueryInternal(connection, out, q, error);
}

} // namespace duckdb_adbc
