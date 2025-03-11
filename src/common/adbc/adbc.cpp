#include "duckdb/common/adbc/adbc.hpp"
#include "duckdb/common/adbc/adbc-init.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"

#include "duckdb.h"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/nanoarrow/nanoarrow.hpp"

#include "duckdb/main/capi/capi_internal.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/main/connection.hpp"
#endif

#include "duckdb/common/adbc/options.h"
#include "duckdb/common/adbc/single_batch_array_stream.hpp"
#include "duckdb/function/table/arrow.hpp"

#include <stdlib.h>
#include <string.h>

// We must leak the symbols of the init function
AdbcStatusCode duckdb_adbc_init(int version, void *driver, struct AdbcError *error) {
	if (!driver) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	auto adbc_driver = static_cast<AdbcDriver *>(driver);

	adbc_driver->DatabaseNew = duckdb_adbc::DatabaseNew;
	adbc_driver->DatabaseSetOption = duckdb_adbc::DatabaseSetOption;
	adbc_driver->DatabaseInit = duckdb_adbc::DatabaseInit;
	adbc_driver->DatabaseRelease = duckdb_adbc::DatabaseRelease;
	adbc_driver->ConnectionNew = duckdb_adbc::ConnectionNew;
	adbc_driver->ConnectionSetOption = duckdb_adbc::ConnectionSetOption;
	adbc_driver->ConnectionInit = duckdb_adbc::ConnectionInit;
	adbc_driver->ConnectionRelease = duckdb_adbc::ConnectionRelease;
	adbc_driver->ConnectionGetTableTypes = duckdb_adbc::ConnectionGetTableTypes;
	adbc_driver->StatementNew = duckdb_adbc::StatementNew;
	adbc_driver->StatementRelease = duckdb_adbc::StatementRelease;
	adbc_driver->StatementBind = duckdb_adbc::StatementBind;
	adbc_driver->StatementBindStream = duckdb_adbc::StatementBindStream;
	adbc_driver->StatementExecuteQuery = duckdb_adbc::StatementExecuteQuery;
	adbc_driver->StatementPrepare = duckdb_adbc::StatementPrepare;
	adbc_driver->StatementSetOption = duckdb_adbc::StatementSetOption;
	adbc_driver->StatementSetSqlQuery = duckdb_adbc::StatementSetSqlQuery;
	adbc_driver->ConnectionGetObjects = duckdb_adbc::ConnectionGetObjects;
	adbc_driver->ConnectionCommit = duckdb_adbc::ConnectionCommit;
	adbc_driver->ConnectionRollback = duckdb_adbc::ConnectionRollback;
	adbc_driver->ConnectionReadPartition = duckdb_adbc::ConnectionReadPartition;
	adbc_driver->StatementExecutePartitions = duckdb_adbc::StatementExecutePartitions;
	adbc_driver->ConnectionGetInfo = duckdb_adbc::ConnectionGetInfo;
	adbc_driver->StatementGetParameterSchema = duckdb_adbc::StatementGetParameterSchema;
	adbc_driver->ConnectionGetTableSchema = duckdb_adbc::ConnectionGetTableSchema;
	return ADBC_STATUS_OK;
}

namespace duckdb_adbc {

enum class IngestionMode { CREATE = 0, APPEND = 1 };

struct DuckDBAdbcStatementWrapper {
	duckdb_connection connection;
	duckdb_arrow result;
	duckdb_prepared_statement statement;
	char *ingestion_table_name;
	char *db_schema;
	ArrowArrayStream ingestion_stream;
	IngestionMode ingestion_mode = IngestionMode::CREATE;
	bool temporary_table = false;
	uint64_t plan_length;
};

static AdbcStatusCode QueryInternal(struct AdbcConnection *connection, struct ArrowArrayStream *out, const char *query,
                                    struct AdbcError *error) {
	AdbcStatement statement;

	auto status = StatementNew(connection, &statement, error);
	if (status != ADBC_STATUS_OK) {
		StatementRelease(&statement, error);
		SetError(error, "unable to initialize statement");
		return status;
	}
	status = StatementSetSqlQuery(&statement, query, error);
	if (status != ADBC_STATUS_OK) {
		StatementRelease(&statement, error);
		SetError(error, "unable to initialize statement");
		return status;
	}
	status = StatementExecuteQuery(&statement, out, nullptr, error);
	if (status != ADBC_STATUS_OK) {
		StatementRelease(&statement, error);
		SetError(error, "unable to initialize statement");
		return status;
	}
	StatementRelease(&statement, error);
	return ADBC_STATUS_OK;
}

struct DuckDBAdbcDatabaseWrapper {
	//! The DuckDB Database Configuration
	duckdb_config config = nullptr;
	//! The DuckDB Database
	duckdb_database database = nullptr;
	//! Path of Disk-Based Database or :memory: database
	std::string path;
};

static void EmptyErrorRelease(AdbcError *error) {
	// The object is valid but doesn't contain any data that needs to be cleaned up
	// Just set the release to nullptr to indicate that it's no longer valid.
	error->release = nullptr;
}

void InitializeADBCError(AdbcError *error) {
	if (!error) {
		return;
	}
	error->message = nullptr;
	// Don't set to nullptr, as that indicates that it's invalid
	error->release = EmptyErrorRelease;
	std::memset(error->sqlstate, '\0', sizeof(error->sqlstate));
	error->vendor_code = -1;
}

AdbcStatusCode CheckResult(const duckdb_state &res, AdbcError *error, const char *error_msg) {
	if (!error) {
		// Error should be a non-null pointer
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (res != DuckDBSuccess) {
		SetError(error, error_msg);
		return ADBC_STATUS_INTERNAL;
	}
	return ADBC_STATUS_OK;
}

AdbcStatusCode DatabaseNew(struct AdbcDatabase *database, struct AdbcError *error) {
	if (!database) {
		SetError(error, "Missing database object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	database->private_data = nullptr;
	// you can't malloc a struct with a non-trivial C++ constructor
	// and std::string has a non-trivial constructor. so we need
	// to use new and delete rather than malloc and free.
	auto wrapper = new (std::nothrow) DuckDBAdbcDatabaseWrapper;
	if (!wrapper) {
		SetError(error, "Allocation error");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	database->private_data = wrapper;
	auto res = duckdb_create_config(&wrapper->config);
	return CheckResult(res, error, "Failed to allocate");
}

AdbcStatusCode DatabaseSetOption(struct AdbcDatabase *database, const char *key, const char *value,
                                 struct AdbcError *error) {
	if (!database) {
		SetError(error, "Missing database object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!key) {
		SetError(error, "Missing key");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}

	auto wrapper = static_cast<DuckDBAdbcDatabaseWrapper *>(database->private_data);
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
		SetError(error, "ADBC Database has an invalid pointer");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	char *errormsg = nullptr;
	// TODO can we set the database path via option, too? Does not look like it...
	auto wrapper = static_cast<DuckDBAdbcDatabaseWrapper *>(database->private_data);
	auto res = duckdb_open_ext(wrapper->path.c_str(), &wrapper->database, wrapper->config, &errormsg);
	auto adbc_result = CheckResult(res, error, errormsg);
	if (errormsg) {
		free(errormsg);
	}
	return adbc_result;
}

AdbcStatusCode DatabaseRelease(struct AdbcDatabase *database, struct AdbcError *error) {

	if (database && database->private_data) {
		auto wrapper = static_cast<DuckDBAdbcDatabaseWrapper *>(database->private_data);

		duckdb_close(&wrapper->database);
		duckdb_destroy_config(&wrapper->config);
		delete wrapper;
		database->private_data = nullptr;
	}
	return ADBC_STATUS_OK;
}

AdbcStatusCode ConnectionGetTableSchema(struct AdbcConnection *connection, const char *catalog, const char *db_schema,
                                        const char *table_name, struct ArrowSchema *schema, struct AdbcError *error) {
	if (!connection) {
		SetError(error, "Connection is not set");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (db_schema == nullptr || strlen(db_schema) == 0) {
		// if schema is not set, we use the default schema
		db_schema = "main";
	}
	if (table_name == nullptr) {
		SetError(error, "AdbcConnectionGetTableSchema: must provide table_name");
		return ADBC_STATUS_INVALID_ARGUMENT;
	} else if (strlen(table_name) == 0) {
		SetError(error, "AdbcConnectionGetTableSchema: must provide table_name");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	ArrowArrayStream arrow_stream;

	std::string query = "SELECT * FROM ";
	if (catalog != nullptr && strlen(catalog) > 0) {
		query += duckdb::KeywordHelper::WriteOptionallyQuoted(catalog) + ".";
	}
	query += duckdb::KeywordHelper::WriteOptionallyQuoted(db_schema) + ".";
	query += duckdb::KeywordHelper::WriteOptionallyQuoted(table_name) + " LIMIT 0;";

	auto success = QueryInternal(connection, &arrow_stream, query.c_str(), error);
	if (success != ADBC_STATUS_OK) {
		return success;
	}
	arrow_stream.get_schema(&arrow_stream, schema);
	arrow_stream.release(&arrow_stream);
	return ADBC_STATUS_OK;
}

AdbcStatusCode ConnectionNew(struct AdbcConnection *connection, struct AdbcError *error) {
	if (!connection) {
		SetError(error, "Missing connection object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}

	connection->private_data = nullptr;
	return ADBC_STATUS_OK;
}

AdbcStatusCode ExecuteQuery(duckdb::Connection *conn, const char *query, struct AdbcError *error) {
	auto res = conn->Query(query);
	if (res->HasError()) {
		auto error_message = "Failed to execute query \"" + std::string(query) + "\": " + res->GetError();
		SetError(error, error_message);
		return ADBC_STATUS_INTERNAL;
	}
	return ADBC_STATUS_OK;
}

AdbcStatusCode ConnectionSetOption(struct AdbcConnection *connection, const char *key, const char *value,
                                   struct AdbcError *error) {
	if (!connection) {
		SetError(error, "Connection is not set");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}

	auto conn = static_cast<duckdb::Connection *>(connection->private_data);
	if (strcmp(key, ADBC_CONNECTION_OPTION_AUTOCOMMIT) == 0) {
		if (strcmp(value, ADBC_OPTION_VALUE_ENABLED) == 0) {
			if (conn->HasActiveTransaction()) {
				AdbcStatusCode status = ExecuteQuery(conn, "COMMIT", error);
				if (status != ADBC_STATUS_OK) {
					return status;
				}
			} else {
				// no-op
			}
		} else if (strcmp(value, ADBC_OPTION_VALUE_DISABLED) == 0) {
			if (conn->HasActiveTransaction()) {
				// no-op
			} else {
				// begin
				AdbcStatusCode status = ExecuteQuery(conn, "START TRANSACTION", error);
				if (status != ADBC_STATUS_OK) {
					return status;
				}
			}
		} else {
			auto error_message = "Invalid connection option value " + std::string(key) + "=" + std::string(value);
			SetError(error, error_message);
			return ADBC_STATUS_INVALID_ARGUMENT;
		}
		return ADBC_STATUS_OK;
	}
	auto error_message =
	    "Unknown connection option " + std::string(key) + "=" + (value ? std::string(value) : "(NULL)");
	SetError(error, error_message);
	return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode ConnectionReadPartition(struct AdbcConnection *connection, const uint8_t *serialized_partition,
                                       size_t serialized_length, struct ArrowArrayStream *out,
                                       struct AdbcError *error) {
	SetError(error, "Read Partitions are not supported in DuckDB");
	return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode StatementExecutePartitions(struct AdbcStatement *statement, struct ArrowSchema *schema,
                                          struct AdbcPartitions *partitions, int64_t *rows_affected,
                                          struct AdbcError *error) {
	SetError(error, "Execute Partitions are not supported in DuckDB");
	return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode ConnectionCommit(struct AdbcConnection *connection, struct AdbcError *error) {
	if (!connection) {
		SetError(error, "Connection is not set");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	auto conn = static_cast<duckdb::Connection *>(connection->private_data);
	if (!conn->HasActiveTransaction()) {
		SetError(error, "No active transaction, cannot commit");
		return ADBC_STATUS_INVALID_STATE;
	}

	AdbcStatusCode status = ExecuteQuery(conn, "COMMIT", error);
	if (status != ADBC_STATUS_OK) {
		return status;
	}
	return ExecuteQuery(conn, "START TRANSACTION", error);
}

AdbcStatusCode ConnectionRollback(struct AdbcConnection *connection, struct AdbcError *error) {
	if (!connection) {
		SetError(error, "Connection is not set");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	auto conn = static_cast<duckdb::Connection *>(connection->private_data);
	if (!conn->HasActiveTransaction()) {
		SetError(error, "No active transaction, cannot rollback");
		return ADBC_STATUS_INVALID_STATE;
	}

	AdbcStatusCode status = ExecuteQuery(conn, "ROLLBACK", error);
	if (status != ADBC_STATUS_OK) {
		return status;
	}
	return ExecuteQuery(conn, "START TRANSACTION", error);
}

enum class AdbcInfoCode : uint32_t {
	VENDOR_NAME,
	VENDOR_VERSION,
	DRIVER_NAME,
	DRIVER_VERSION,
	DRIVER_ARROW_VERSION,
	UNRECOGNIZED // always the last entry of the enum
};

static AdbcInfoCode ConvertToInfoCode(uint32_t info_code) {
	switch (info_code) {
	case 0:
		return AdbcInfoCode::VENDOR_NAME;
	case 1:
		return AdbcInfoCode::VENDOR_VERSION;
	case 2:
		return AdbcInfoCode::DRIVER_NAME;
	case 3:
		return AdbcInfoCode::DRIVER_VERSION;
	case 4:
		return AdbcInfoCode::DRIVER_ARROW_VERSION;
	default:
		return AdbcInfoCode::UNRECOGNIZED;
	}
}

AdbcStatusCode ConnectionGetInfo(struct AdbcConnection *connection, const uint32_t *info_codes,
                                 size_t info_codes_length, struct ArrowArrayStream *out, struct AdbcError *error) {
	if (!connection) {
		SetError(error, "Missing connection object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!connection->private_data) {
		SetError(error, "Connection is invalid");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!out) {
		SetError(error, "Output parameter was not provided");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}

	// If 'info_codes' is NULL, we should output all the info codes we recognize
	size_t length = info_codes ? info_codes_length : static_cast<size_t>(AdbcInfoCode::UNRECOGNIZED);

	duckdb::string q = R"EOF(
		select
			name::UINTEGER as info_name,
			info::UNION(
				string_value VARCHAR,
				bool_value BOOL,
				int64_value BIGINT,
				int32_bitmask INTEGER,
				string_list VARCHAR[],
				int32_to_int32_list_map MAP(INTEGER, INTEGER[])
			) as info_value from values
	)EOF";

	duckdb::string results = "";

	for (size_t i = 0; i < length; i++) {
		auto code = duckdb::NumericCast<uint32_t>(info_codes ? info_codes[i] : i);
		auto info_code = ConvertToInfoCode(code);
		switch (info_code) {
		case AdbcInfoCode::VENDOR_NAME: {
			results += "(0, 'duckdb'),";
			break;
		}
		case AdbcInfoCode::VENDOR_VERSION: {
			results += duckdb::StringUtil::Format("(1, '%s'),", duckdb_library_version());
			break;
		}
		case AdbcInfoCode::DRIVER_NAME: {
			results += "(2, 'ADBC DuckDB Driver'),";
			break;
		}
		case AdbcInfoCode::DRIVER_VERSION: {
			// TODO: fill in driver version
			results += "(3, '(unknown)'),";
			break;
		}
		case AdbcInfoCode::DRIVER_ARROW_VERSION: {
			// TODO: fill in arrow version
			results += "(4, '(unknown)'),";
			break;
		}
		case AdbcInfoCode::UNRECOGNIZED: {
			// Unrecognized codes are not an error, just ignored
			continue;
		}
		default: {
			// Codes that we have implemented but not handled here are a developer error
			SetError(error, "Info code recognized but not handled");
			return ADBC_STATUS_INTERNAL;
		}
		}
	}
	if (results.empty()) {
		// Add a group of values so the query parses
		q += "(NULL, NULL)";
	} else {
		q += results;
	}
	q += " tbl(name, info)";
	if (results.empty()) {
		// Add an impossible where clause to return an empty result set
		q += " where true = false";
	}
	return QueryInternal(connection, out, q.c_str(), error);
}

AdbcStatusCode ConnectionInit(struct AdbcConnection *connection, struct AdbcDatabase *database,
                              struct AdbcError *error) {
	if (!database) {
		SetError(error, "Missing database object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!database->private_data) {
		SetError(error, "Invalid database");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!connection) {
		SetError(error, "Missing connection object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	auto database_wrapper = static_cast<DuckDBAdbcDatabaseWrapper *>(database->private_data);

	connection->private_data = nullptr;
	auto res =
	    duckdb_connect(database_wrapper->database, reinterpret_cast<duckdb_connection *>(&connection->private_data));
	return CheckResult(res, error, "Failed to connect to Database");
}

AdbcStatusCode ConnectionRelease(struct AdbcConnection *connection, struct AdbcError *error) {
	if (connection && connection->private_data) {
		duckdb_disconnect(reinterpret_cast<duckdb_connection *>(&connection->private_data));
		connection->private_data = nullptr;
	}
	return ADBC_STATUS_OK;
}

// some stream callbacks

static int get_schema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
	if (!stream || !stream->private_data || !out) {
		return DuckDBError;
	}
	return duckdb_query_arrow_schema(static_cast<duckdb_arrow>(stream->private_data),
	                                 reinterpret_cast<duckdb_arrow_schema *>(&out));
}

static int get_next(struct ArrowArrayStream *stream, struct ArrowArray *out) {
	if (!stream || !stream->private_data || !out) {
		return DuckDBError;
	}
	out->release = nullptr;

	return duckdb_query_arrow_array(static_cast<duckdb_arrow>(stream->private_data),
	                                reinterpret_cast<duckdb_arrow_array *>(&out));
}

void release(struct ArrowArrayStream *stream) {
	if (!stream || !stream->release) {
		return;
	}
	if (stream->private_data) {
		duckdb_destroy_arrow(reinterpret_cast<duckdb_arrow *>(&stream->private_data));
		stream->private_data = nullptr;
	}
	stream->release = nullptr;
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

duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper> stream_produce(uintptr_t factory_ptr,
                                                                   duckdb::ArrowStreamParameters &parameters) {

	// TODO this will ignore any projections or filters but since we don't expose the scan it should be sort of fine
	auto res = duckdb::make_uniq<duckdb::ArrowArrayStreamWrapper>();
	res->arrow_array_stream = *reinterpret_cast<ArrowArrayStream *>(factory_ptr);
	return res;
}

void stream_schema(ArrowArrayStream *stream, ArrowSchema &schema) {
	stream->get_schema(stream, &schema);
}

AdbcStatusCode Ingest(duckdb_connection connection, const char *table_name, const char *schema,
                      struct ArrowArrayStream *input, struct AdbcError *error, IngestionMode ingestion_mode,
                      bool temporary) {

	if (!connection) {
		SetError(error, "Missing connection object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!input) {
		SetError(error, "Missing input arrow stream pointer");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!table_name) {
		SetError(error, "Missing database object name");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (schema && temporary) {
		// Temporary option is not supported with ADBC_INGEST_OPTION_TARGET_DB_SCHEMA or
		// ADBC_INGEST_OPTION_TARGET_CATALOG
		SetError(error, "Temporary option is not supported with schema");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}

	auto cconn = reinterpret_cast<duckdb::Connection *>(connection);

	auto arrow_scan =
	    cconn->TableFunction("arrow_scan", {duckdb::Value::POINTER(reinterpret_cast<uintptr_t>(input)),
	                                        duckdb::Value::POINTER(reinterpret_cast<uintptr_t>(stream_produce)),
	                                        duckdb::Value::POINTER(reinterpret_cast<uintptr_t>(stream_schema))});
	try {
		switch (ingestion_mode) {
		case IngestionMode::CREATE:
			if (schema) {
				arrow_scan->Create(schema, table_name, temporary);
			} else {
				arrow_scan->Create(table_name, temporary);
			}
			break;
		case IngestionMode::APPEND: {
			arrow_scan->CreateView("temp_adbc_view", true, true);
			std::string query = "insert into ";
			if (schema) {
				query += duckdb::KeywordHelper::WriteOptionallyQuoted(schema) + ".";
			}
			query += duckdb::KeywordHelper::WriteOptionallyQuoted(table_name);
			query += " select * from temp_adbc_view";
			auto result = cconn->Query(query);
			break;
		}
		}
		// After creating a table, the arrow array stream is released. Hence we must set it as released to avoid
		// double-releasing it
		input->release = nullptr;
	} catch (std::exception &ex) {
		if (error) {
			duckdb::ErrorData parsed_error(ex);
			error->message = strdup(parsed_error.RawMessage().c_str());
		}
		return ADBC_STATUS_INTERNAL;
	} catch (...) {
		return ADBC_STATUS_INTERNAL;
	}
	return ADBC_STATUS_OK;
}

AdbcStatusCode StatementNew(struct AdbcConnection *connection, struct AdbcStatement *statement,
                            struct AdbcError *error) {
	if (!connection) {
		SetError(error, "Missing connection object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!connection->private_data) {
		SetError(error, "Invalid connection object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!statement) {
		SetError(error, "Missing statement object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}

	statement->private_data = nullptr;

	auto statement_wrapper = static_cast<DuckDBAdbcStatementWrapper *>(malloc(sizeof(DuckDBAdbcStatementWrapper)));
	if (!statement_wrapper) {
		SetError(error, "Allocation error");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}

	statement->private_data = statement_wrapper;
	statement_wrapper->connection = static_cast<duckdb_connection>(connection->private_data);
	statement_wrapper->statement = nullptr;
	statement_wrapper->result = nullptr;
	statement_wrapper->ingestion_stream.release = nullptr;
	statement_wrapper->ingestion_table_name = nullptr;
	statement_wrapper->db_schema = nullptr;
	statement_wrapper->temporary_table = false;

	statement_wrapper->ingestion_mode = IngestionMode::CREATE;
	return ADBC_STATUS_OK;
}

AdbcStatusCode StatementRelease(struct AdbcStatement *statement, struct AdbcError *error) {
	if (!statement || !statement->private_data) {
		return ADBC_STATUS_OK;
	}
	auto wrapper = static_cast<DuckDBAdbcStatementWrapper *>(statement->private_data);
	if (wrapper->statement) {
		duckdb_destroy_prepare(&wrapper->statement);
		wrapper->statement = nullptr;
	}
	if (wrapper->result) {
		duckdb_destroy_arrow(&wrapper->result);
		wrapper->result = nullptr;
	}
	if (wrapper->ingestion_stream.release) {
		wrapper->ingestion_stream.release(&wrapper->ingestion_stream);
		wrapper->ingestion_stream.release = nullptr;
	}
	if (wrapper->ingestion_table_name) {
		free(wrapper->ingestion_table_name);
		wrapper->ingestion_table_name = nullptr;
	}
	if (wrapper->db_schema) {
		free(wrapper->db_schema);
		wrapper->db_schema = nullptr;
	}
	free(statement->private_data);
	statement->private_data = nullptr;
	return ADBC_STATUS_OK;
}

AdbcStatusCode StatementGetParameterSchema(struct AdbcStatement *statement, struct ArrowSchema *schema,
                                           struct AdbcError *error) {
	if (!statement) {
		SetError(error, "Missing statement object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!statement->private_data) {
		SetError(error, "Invalid statement object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!schema) {
		SetError(error, "Missing schema object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	auto wrapper = static_cast<DuckDBAdbcStatementWrapper *>(statement->private_data);
	// TODO: we might want to cache this, but then we need to return a deep copy anyways.., so I'm not sure if that
	// would be worth the extra management
	auto res = duckdb_prepared_arrow_schema(wrapper->statement, reinterpret_cast<duckdb_arrow_schema *>(&schema));
	if (res != DuckDBSuccess) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	return ADBC_STATUS_OK;
}

AdbcStatusCode GetPreparedParameters(duckdb_connection connection, duckdb::unique_ptr<duckdb::QueryResult> &result,
                                     ArrowArrayStream *input, AdbcError *error) {

	auto cconn = reinterpret_cast<duckdb::Connection *>(connection);

	try {
		auto arrow_scan =
		    cconn->TableFunction("arrow_scan", {duckdb::Value::POINTER(reinterpret_cast<uintptr_t>(input)),
		                                        duckdb::Value::POINTER(reinterpret_cast<uintptr_t>(stream_produce)),
		                                        duckdb::Value::POINTER(reinterpret_cast<uintptr_t>(stream_schema))});
		result = arrow_scan->Execute();
		// After creating a table, the arrow array stream is released. Hence we must set it as released to avoid
		// double-releasing it
		input->release = nullptr;
	} catch (std::exception &ex) {
		if (error) {
			::duckdb::ErrorData parsed_error(ex);
			error->message = strdup(parsed_error.RawMessage().c_str());
		}
		return ADBC_STATUS_INTERNAL;
	} catch (...) {
		return ADBC_STATUS_INTERNAL;
	}
	return ADBC_STATUS_OK;
}

static AdbcStatusCode IngestToTableFromBoundStream(DuckDBAdbcStatementWrapper *statement, AdbcError *error) {
	// See ADBC_INGEST_OPTION_TARGET_TABLE
	D_ASSERT(statement->ingestion_stream.release);
	D_ASSERT(statement->ingestion_table_name);

	// Take the input stream from the statement
	auto stream = statement->ingestion_stream;
	statement->ingestion_stream.release = nullptr;

	// Ingest into a table from the bound stream
	return Ingest(statement->connection, statement->ingestion_table_name, statement->db_schema, &stream, error,
	              statement->ingestion_mode, statement->temporary_table);
}

AdbcStatusCode StatementExecuteQuery(struct AdbcStatement *statement, struct ArrowArrayStream *out,
                                     int64_t *rows_affected, struct AdbcError *error) {
	if (!statement) {
		SetError(error, "Missing statement object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!statement->private_data) {
		SetError(error, "Invalid statement object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	auto wrapper = static_cast<DuckDBAdbcStatementWrapper *>(statement->private_data);

	// TODO: Set affected rows, careful with early return
	if (rows_affected) {
		*rows_affected = 0;
	}

	const auto has_stream = wrapper->ingestion_stream.release != nullptr;
	const auto to_table = wrapper->ingestion_table_name != nullptr;

	if (has_stream && to_table) {
		return IngestToTableFromBoundStream(wrapper, error);
	}
	if (has_stream) {
		// A stream was bound to the statement, use that to bind parameters
		duckdb::unique_ptr<duckdb::QueryResult> result;
		ArrowArrayStream stream = wrapper->ingestion_stream;
		wrapper->ingestion_stream.release = nullptr;
		auto adbc_res = GetPreparedParameters(wrapper->connection, result, &stream, error);
		if (adbc_res != ADBC_STATUS_OK) {
			return adbc_res;
		}
		if (!result) {
			return ADBC_STATUS_INVALID_ARGUMENT;
		}
		duckdb::unique_ptr<duckdb::DataChunk> chunk;
		auto prepared_statement_params =
		    reinterpret_cast<duckdb::PreparedStatementWrapper *>(wrapper->statement)->statement->named_param_map.size();

		while ((chunk = result->Fetch()) != nullptr) {
			if (chunk->size() == 0) {
				SetError(error, "Please provide a non-empty chunk to be bound");
				return ADBC_STATUS_INVALID_ARGUMENT;
			}
			if (chunk->size() != 1) {
				// TODO: add support for binding multiple rows
				SetError(error, "Binding multiple rows at once is not supported yet");
				return ADBC_STATUS_NOT_IMPLEMENTED;
			}
			if (chunk->ColumnCount() > prepared_statement_params) {
				SetError(error, "Input data has more column than prepared statement has parameters");
				return ADBC_STATUS_INVALID_ARGUMENT;
			}
			duckdb_clear_bindings(wrapper->statement);
			for (idx_t col_idx = 0; col_idx < chunk->ColumnCount(); col_idx++) {
				auto val = chunk->GetValue(col_idx, 0);
				auto duck_val = reinterpret_cast<duckdb_value>(&val);
				auto res = duckdb_bind_value(wrapper->statement, 1 + col_idx, duck_val);
				if (res != DuckDBSuccess) {
					SetError(error, duckdb_prepare_error(wrapper->statement));
					return ADBC_STATUS_INVALID_ARGUMENT;
				}
			}

			auto res = duckdb_execute_prepared_arrow(wrapper->statement, &wrapper->result);
			if (res != DuckDBSuccess) {
				SetError(error, duckdb_query_arrow_error(wrapper->result));
				return ADBC_STATUS_INVALID_ARGUMENT;
			}
		}
	} else {
		auto res = duckdb_execute_prepared_arrow(wrapper->statement, &wrapper->result);
		if (res != DuckDBSuccess) {
			SetError(error, duckdb_query_arrow_error(wrapper->result));
			return ADBC_STATUS_INVALID_ARGUMENT;
		}
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
	if (!statement) {
		SetError(error, "Missing statement object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!statement->private_data) {
		SetError(error, "Invalid statement object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	return ADBC_STATUS_OK;
}

AdbcStatusCode StatementSetSqlQuery(struct AdbcStatement *statement, const char *query, struct AdbcError *error) {
	if (!statement) {
		SetError(error, "Missing statement object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!statement->private_data) {
		SetError(error, "Invalid statement object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!query) {
		SetError(error, "Missing query");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}

	auto wrapper = static_cast<DuckDBAdbcStatementWrapper *>(statement->private_data);
	if (wrapper->ingestion_stream.release) {
		// Release any resources currently held by the ingestion stream before we overwrite it
		wrapper->ingestion_stream.release(&wrapper->ingestion_stream);
		wrapper->ingestion_stream.release = nullptr;
	}
	if (wrapper->statement) {
		duckdb_destroy_prepare(&wrapper->statement);
		wrapper->statement = nullptr;
	}
	duckdb_extracted_statements extracted_statements = nullptr;
	auto extract_statements_size = duckdb_extract_statements(wrapper->connection, query, &extracted_statements);
	auto error_msg_extract_statements = duckdb_extract_statements_error(extracted_statements);
	if (error_msg_extract_statements != nullptr) {
		// Things went wrong when executing internal prepared statement
		duckdb_destroy_extracted(&extracted_statements);
		SetError(error, error_msg_extract_statements);
		return ADBC_STATUS_INTERNAL;
	}
	// Now lets loop over the statements, and execute every one
	for (idx_t i = 0; i < extract_statements_size - 1; i++) {
		duckdb_prepared_statement statement_internal = nullptr;
		auto res =
		    duckdb_prepare_extracted_statement(wrapper->connection, extracted_statements, i, &statement_internal);
		auto error_msg = duckdb_prepare_error(statement_internal);
		auto adbc_status = CheckResult(res, error, error_msg);
		if (adbc_status != ADBC_STATUS_OK) {
			duckdb_destroy_prepare(&statement_internal);
			duckdb_destroy_extracted(&extracted_statements);
			return adbc_status;
		}
		// Execute
		duckdb_arrow out_result;
		res = duckdb_execute_prepared_arrow(statement_internal, &out_result);
		if (res != DuckDBSuccess) {
			SetError(error, duckdb_query_arrow_error(out_result));
			duckdb_destroy_arrow(&out_result);
			duckdb_destroy_prepare(&statement_internal);
			duckdb_destroy_extracted(&extracted_statements);
			return ADBC_STATUS_INVALID_ARGUMENT;
		}
		duckdb_destroy_arrow(&out_result);
		duckdb_destroy_prepare(&statement_internal);
	}

	// Final statement (returned to caller)
	auto res = duckdb_prepare_extracted_statement(wrapper->connection, extracted_statements,
	                                              extract_statements_size - 1, &wrapper->statement);
	auto error_msg = duckdb_prepare_error(wrapper->statement);
	duckdb_destroy_extracted(&extracted_statements);
	return CheckResult(res, error, error_msg);
}

AdbcStatusCode StatementBind(struct AdbcStatement *statement, struct ArrowArray *values, struct ArrowSchema *schemas,
                             struct AdbcError *error) {
	if (!statement) {
		SetError(error, "Missing statement object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!statement->private_data) {
		SetError(error, "Invalid statement object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!values) {
		SetError(error, "Missing values object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!schemas) {
		SetError(error, "Invalid schemas object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}

	auto wrapper = static_cast<DuckDBAdbcStatementWrapper *>(statement->private_data);
	if (wrapper->ingestion_stream.release) {
		// Free the stream that was previously bound
		wrapper->ingestion_stream.release(&wrapper->ingestion_stream);
	}
	auto status = BatchToArrayStream(values, schemas, &wrapper->ingestion_stream, error);
	return status;
}

AdbcStatusCode StatementBindStream(struct AdbcStatement *statement, struct ArrowArrayStream *values,
                                   struct AdbcError *error) {
	if (!statement) {
		SetError(error, "Missing statement object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!statement->private_data) {
		SetError(error, "Invalid statement object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!values) {
		SetError(error, "Missing values object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}

	auto wrapper = static_cast<DuckDBAdbcStatementWrapper *>(statement->private_data);
	if (wrapper->ingestion_stream.release) {
		// Release any resources currently held by the ingestion stream before we overwrite it
		wrapper->ingestion_stream.release(&wrapper->ingestion_stream);
	}
	wrapper->ingestion_stream = *values;
	values->release = nullptr;
	return ADBC_STATUS_OK;
}

AdbcStatusCode StatementSetOption(struct AdbcStatement *statement, const char *key, const char *value,
                                  struct AdbcError *error) {
	if (!statement) {
		SetError(error, "Missing statement object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!statement->private_data) {
		SetError(error, "Invalid statement object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!key) {
		SetError(error, "Missing key object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}

	auto wrapper = static_cast<DuckDBAdbcStatementWrapper *>(statement->private_data);

	if (strcmp(key, ADBC_INGEST_OPTION_TARGET_TABLE) == 0) {
		wrapper->ingestion_table_name = strdup(value);
		wrapper->temporary_table = false;
		return ADBC_STATUS_OK;
	}
	if (strcmp(key, ADBC_INGEST_OPTION_TEMPORARY) == 0) {
		if (strcmp(value, ADBC_OPTION_VALUE_ENABLED) == 0) {
			if (wrapper->db_schema) {
				SetError(error, "Temporary option is not supported with schema");
				return ADBC_STATUS_INVALID_ARGUMENT;
			}
			wrapper->temporary_table = true;
			return ADBC_STATUS_OK;
		} else if (strcmp(value, ADBC_OPTION_VALUE_DISABLED) == 0) {
			wrapper->temporary_table = false;
			return ADBC_STATUS_OK;
		} else {
			SetError(
			    error,
			    "ADBC_INGEST_OPTION_TEMPORARY, can only be ADBC_OPTION_VALUE_ENABLED or ADBC_OPTION_VALUE_DISABLED");
			return ADBC_STATUS_INVALID_ARGUMENT;
		}
	}

	if (strcmp(key, ADBC_INGEST_OPTION_TARGET_DB_SCHEMA) == 0) {
		if (wrapper->temporary_table) {
			SetError(error, "Temporary option is not supported with schema");
			return ADBC_STATUS_INVALID_ARGUMENT;
		}
		wrapper->db_schema = strdup(value);
		return ADBC_STATUS_OK;
	}

	if (strcmp(key, ADBC_INGEST_OPTION_MODE) == 0) {
		if (strcmp(value, ADBC_INGEST_OPTION_MODE_CREATE) == 0) {
			wrapper->ingestion_mode = IngestionMode::CREATE;
			return ADBC_STATUS_OK;
		} else if (strcmp(value, ADBC_INGEST_OPTION_MODE_APPEND) == 0) {
			wrapper->ingestion_mode = IngestionMode::APPEND;
			return ADBC_STATUS_OK;
		} else {
			SetError(error, "Invalid ingestion mode");
			return ADBC_STATUS_INVALID_ARGUMENT;
		}
	}
	std::stringstream ss;
	ss << "Statement Set Option " << key << " is not yet accepted by DuckDB";
	SetError(error, ss.str());
	return ADBC_STATUS_INVALID_ARGUMENT;
}

AdbcStatusCode ConnectionGetObjects(struct AdbcConnection *connection, int depth, const char *catalog,
                                    const char *db_schema, const char *table_name, const char **table_type,
                                    const char *column_name, struct ArrowArrayStream *out, struct AdbcError *error) {
	if (table_type != nullptr) {
		SetError(error, "Table types parameter not yet supported");
		return ADBC_STATUS_NOT_IMPLEMENTED;
	}

	std::string catalog_filter = catalog ? catalog : "%";
	std::string db_schema_filter = db_schema ? db_schema : "%";
	std::string table_name_filter = table_name ? table_name : "%";
	std::string column_name_filter = column_name ? column_name : "%";

	std::string query;
	switch (depth) {
	case ADBC_OBJECT_DEPTH_CATALOGS:
		// Return metadata on catalogs.
		query = duckdb::StringUtil::Format(R"(
				SELECT
					catalog_name,
					[]::STRUCT(
						db_schema_name VARCHAR,
						db_schema_tables STRUCT(
							table_name VARCHAR,
							table_type VARCHAR,
							table_columns STRUCT(
								column_name VARCHAR,
								ordinal_position INTEGER,
								remarks VARCHAR,
								xdbc_data_type SMALLINT,
								xdbc_type_name VARCHAR,
								xdbc_column_size INTEGER,
								xdbc_decimal_digits SMALLINT,
								xdbc_num_prec_radix SMALLINT,
								xdbc_nullable SMALLINT,
								xdbc_column_def VARCHAR,
								xdbc_sql_data_type SMALLINT,
								xdbc_datetime_sub SMALLINT,
								xdbc_char_octet_length INTEGER,
								xdbc_is_nullable VARCHAR,
								xdbc_scope_catalog VARCHAR,
								xdbc_scope_schema VARCHAR,
								xdbc_scope_table VARCHAR,
								xdbc_is_autoincrement BOOLEAN,
								xdbc_is_generatedcolumn BOOLEAN
							)[],
							table_constraints STRUCT(
								constraint_name VARCHAR,
								constraint_type VARCHAR,
								constraint_column_names VARCHAR[],
								constraint_column_usage STRUCT(fk_catalog VARCHAR, fk_db_schema VARCHAR, fk_table VARCHAR, fk_column_name VARCHAR)[]
							)[]
						)[]
					)[] catalog_db_schemas
				FROM
					information_schema.schemata
				WHERE catalog_name LIKE '%s'
				GROUP BY catalog_name
				)",
		                                   catalog_filter);
		break;
	case ADBC_OBJECT_DEPTH_DB_SCHEMAS:
		// Return metadata on catalogs and schemas.
		query = duckdb::StringUtil::Format(R"(
				WITH db_schemas AS (
					SELECT
						catalog_name,
						schema_name,
					FROM information_schema.schemata
					WHERE schema_name LIKE '%s'
				)

				SELECT
					catalog_name,
					LIST({
						db_schema_name: schema_name,
						db_schema_tables: []::STRUCT(
							table_name VARCHAR,
							table_type VARCHAR,
							table_columns STRUCT(
								column_name VARCHAR,
								ordinal_position INTEGER,
								remarks VARCHAR,
								xdbc_data_type SMALLINT,
								xdbc_type_name VARCHAR,
								xdbc_column_size INTEGER,
								xdbc_decimal_digits SMALLINT,
								xdbc_num_prec_radix SMALLINT,
								xdbc_nullable SMALLINT,
								xdbc_column_def VARCHAR,
								xdbc_sql_data_type SMALLINT,
								xdbc_datetime_sub SMALLINT,
								xdbc_char_octet_length INTEGER,
								xdbc_is_nullable VARCHAR,
								xdbc_scope_catalog VARCHAR,
								xdbc_scope_schema VARCHAR,
								xdbc_scope_table VARCHAR,
								xdbc_is_autoincrement BOOLEAN,
								xdbc_is_generatedcolumn BOOLEAN
							)[],
							table_constraints STRUCT(
								constraint_name VARCHAR,
								constraint_type VARCHAR,
								constraint_column_names VARCHAR[],
								constraint_column_usage STRUCT(fk_catalog VARCHAR, fk_db_schema VARCHAR, fk_table VARCHAR, fk_column_name VARCHAR)[]
							)[]
						)[],
					}) FILTER (dbs.schema_name is not null) catalog_db_schemas
				FROM
					information_schema.schemata
				LEFT JOIN db_schemas dbs
				USING (catalog_name, schema_name)
				WHERE catalog_name LIKE '%s'
				GROUP BY catalog_name
				)",
		                                   db_schema_filter, catalog_filter);
		break;
	case ADBC_OBJECT_DEPTH_TABLES:
		// Return metadata on catalogs, schemas, and tables.
		query = duckdb::StringUtil::Format(R"(
				WITH tables AS (
					SELECT
						table_catalog catalog_name,
						table_schema schema_name,
						LIST({
							table_name: table_name,
							table_type: table_type,
							table_columns: []::STRUCT(
								column_name VARCHAR,
								ordinal_position INTEGER,
								remarks VARCHAR,
								xdbc_data_type SMALLINT,
								xdbc_type_name VARCHAR,
								xdbc_column_size INTEGER,
								xdbc_decimal_digits SMALLINT,
								xdbc_num_prec_radix SMALLINT,
								xdbc_nullable SMALLINT,
								xdbc_column_def VARCHAR,
								xdbc_sql_data_type SMALLINT,
								xdbc_datetime_sub SMALLINT,
								xdbc_char_octet_length INTEGER,
								xdbc_is_nullable VARCHAR,
								xdbc_scope_catalog VARCHAR,
								xdbc_scope_schema VARCHAR,
								xdbc_scope_table VARCHAR,
								xdbc_is_autoincrement BOOLEAN,
								xdbc_is_generatedcolumn BOOLEAN
							)[],
							table_constraints: []::STRUCT(
								constraint_name VARCHAR,
								constraint_type VARCHAR,
								constraint_column_names VARCHAR[],
								constraint_column_usage STRUCT(fk_catalog VARCHAR, fk_db_schema VARCHAR, fk_table VARCHAR, fk_column_name VARCHAR)[]
							)[],
						}) db_schema_tables
					FROM information_schema.tables
					WHERE table_name LIKE '%s'
					GROUP BY table_catalog, table_schema
				),
				db_schemas AS (
					SELECT
						catalog_name,
						schema_name,
						db_schema_tables,
					FROM information_schema.schemata
					LEFT JOIN tables
					USING (catalog_name, schema_name)
					WHERE schema_name LIKE '%s'
				)

				SELECT
					catalog_name,
					LIST({
						db_schema_name: schema_name,
						db_schema_tables: db_schema_tables,
					}) FILTER (dbs.schema_name is not null) catalog_db_schemas
				FROM
					information_schema.schemata
				LEFT JOIN db_schemas dbs
				USING (catalog_name, schema_name)
				WHERE catalog_name LIKE '%s'
				GROUP BY catalog_name
				)",
		                                   table_name_filter, db_schema_filter, catalog_filter);
		break;
	case ADBC_OBJECT_DEPTH_COLUMNS:
		// Return metadata on catalogs, schemas, tables, and columns.
		query = duckdb::StringUtil::Format(R"(
				WITH columns AS (
					SELECT
						table_catalog,
						table_schema,
						table_name,
						LIST({
							column_name: column_name,
							ordinal_position: ordinal_position,
							remarks : '',
							xdbc_data_type: NULL::SMALLINT,
							xdbc_type_name: NULL::VARCHAR,
							xdbc_column_size: NULL::INTEGER,
							xdbc_decimal_digits: NULL::SMALLINT,
							xdbc_num_prec_radix: NULL::SMALLINT,
							xdbc_nullable: NULL::SMALLINT,
							xdbc_column_def: NULL::VARCHAR,
							xdbc_sql_data_type: NULL::SMALLINT,
							xdbc_datetime_sub: NULL::SMALLINT,
							xdbc_char_octet_length: NULL::INTEGER,
							xdbc_is_nullable: NULL::VARCHAR,
							xdbc_scope_catalog: NULL::VARCHAR,
							xdbc_scope_schema: NULL::VARCHAR,
							xdbc_scope_table: NULL::VARCHAR,
							xdbc_is_autoincrement: NULL::BOOLEAN,
							xdbc_is_generatedcolumn: NULL::BOOLEAN,
						}) table_columns
					FROM information_schema.columns
					WHERE column_name LIKE '%s'
					GROUP BY table_catalog, table_schema, table_name
				),
				constraints AS (
					SELECT
						table_catalog,
						table_schema,
						table_name,
						LIST(
							{
								constraint_name: constraint_name,
								constraint_type: constraint_type,
								constraint_column_names: []::VARCHAR[],
								constraint_column_usage: []::STRUCT(fk_catalog VARCHAR, fk_db_schema VARCHAR, fk_table VARCHAR, fk_column_name VARCHAR)[],
							}
						) table_constraints
					FROM information_schema.table_constraints
					GROUP BY table_catalog, table_schema, table_name
				),
				tables AS (
					SELECT
						table_catalog catalog_name,
						table_schema schema_name,
						LIST({
							table_name: table_name,
							table_type: table_type,
							table_columns: table_columns,
							table_constraints: table_constraints,
						}) db_schema_tables
					FROM information_schema.tables
					LEFT JOIN columns
					USING (table_catalog, table_schema, table_name)
					LEFT JOIN constraints
					USING (table_catalog, table_schema, table_name)
					WHERE table_name LIKE '%s'
					GROUP BY table_catalog, table_schema
				),
				db_schemas AS (
					SELECT
						catalog_name,
						schema_name,
						db_schema_tables,
					FROM information_schema.schemata
					LEFT JOIN tables
					USING (catalog_name, schema_name)
					WHERE schema_name LIKE '%s'
				)

				SELECT
					catalog_name,
					LIST({
						db_schema_name: schema_name,
						db_schema_tables: db_schema_tables,
					}) FILTER (dbs.schema_name is not null) catalog_db_schemas
				FROM
					information_schema.schemata
				LEFT JOIN db_schemas dbs
				USING (catalog_name, schema_name)
				WHERE catalog_name LIKE '%s'
				GROUP BY catalog_name
				)",
		                                   column_name_filter, table_name_filter, db_schema_filter, catalog_filter);
		break;
	default:
		SetError(error, "Invalid value of Depth");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}

	return QueryInternal(connection, out, query.c_str(), error);
}

AdbcStatusCode ConnectionGetTableTypes(struct AdbcConnection *connection, struct ArrowArrayStream *out,
                                       struct AdbcError *error) {
	const auto q = "SELECT DISTINCT table_type FROM information_schema.tables ORDER BY table_type";
	return QueryInternal(connection, out, q, error);
}

} // namespace duckdb_adbc
