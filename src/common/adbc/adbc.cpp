#include "duckdb/common/adbc/adbc.hpp"
#include "duckdb/common/adbc/adbc-init.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"

#include "duckdb.h"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/nanoarrow/nanoarrow.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/common/adbc/options.h"
#include "duckdb/common/adbc/single_batch_array_stream.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/common/adbc/wrappers.hpp"
#include <stdlib.h>
#include <string.h>

#include "duckdb/main/prepared_statement_data.hpp"

#include "duckdb/parser/keyword_helper.hpp"

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
	duckdb_prepared_statement statement;
	char *ingestion_table_name;
	char *db_schema;
	ArrowArrayStream ingestion_stream;
	IngestionMode ingestion_mode = IngestionMode::CREATE;
	bool temporary_table = false;
	uint64_t plan_length;
};

struct DuckDBAdbcStreamWrapper {
	duckdb_result result;
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

	auto connection_wrapper = new duckdb::DuckDBAdbcConnectionWrapper();
	connection_wrapper->connection = nullptr;
	connection->private_data = connection_wrapper;
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

AdbcStatusCode InternalSetOption(duckdb::Connection &conn, std::unordered_map<std::string, std::string> &options,
                                 struct AdbcError *error) {
	// If we got here, the options have already been validated and are acceptable
	for (auto &option : options) {
		if (strcmp(option.first.c_str(), ADBC_CONNECTION_OPTION_AUTOCOMMIT) == 0) {
			if (strcmp(option.second.c_str(), ADBC_OPTION_VALUE_ENABLED) == 0) {
				if (conn.HasActiveTransaction()) {
					AdbcStatusCode status = ExecuteQuery(&conn, "COMMIT", error);
					if (status != ADBC_STATUS_OK) {
						options.clear();
						return status;
					}
				}
			} else if (strcmp(option.second.c_str(), ADBC_OPTION_VALUE_DISABLED) == 0) {
				if (!conn.HasActiveTransaction()) {
					AdbcStatusCode status = ExecuteQuery(&conn, "START TRANSACTION", error);
					if (status != ADBC_STATUS_OK) {
						options.clear();
						return status;
					}
				}
			}
		}
	}
	options.clear();
	return ADBC_STATUS_OK;
}
AdbcStatusCode ConnectionSetOption(struct AdbcConnection *connection, const char *key, const char *value,
                                   struct AdbcError *error) {
	if (!connection) {
		SetError(error, "Connection is not set");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	std::string key_string = std::string(key);
	std::string key_value = std::string(value);
	auto conn_wrapper = static_cast<duckdb::DuckDBAdbcConnectionWrapper *>(connection->private_data);
	if (strcmp(key, ADBC_CONNECTION_OPTION_AUTOCOMMIT) == 0) {
		if (strcmp(value, ADBC_OPTION_VALUE_ENABLED) == 0) {
			conn_wrapper->options[key_string] = key_value;
		} else if (strcmp(value, ADBC_OPTION_VALUE_DISABLED) == 0) {
			conn_wrapper->options[key_string] = key_value;
		} else {
			auto error_message = "Invalid connection option value " + std::string(key) + "=" + std::string(value);
			SetError(error, error_message);
			return ADBC_STATUS_INVALID_ARGUMENT;
		}
	} else {
		// This is an unknown option to the DuckDB driver
		auto error_message =
		    "Unknown connection option " + std::string(key) + "=" + (value ? std::string(value) : "(NULL)");
		SetError(error, error_message);
		return ADBC_STATUS_NOT_IMPLEMENTED;
	}
	if (!conn_wrapper->connection) {
		// If the connection has not yet been initialized, we just return here.
		return ADBC_STATUS_OK;
	}
	auto conn = reinterpret_cast<duckdb::Connection *>(conn_wrapper->connection);
	return InternalSetOption(*conn, conn_wrapper->options, error);
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
	auto conn_wrapper = static_cast<duckdb::DuckDBAdbcConnectionWrapper *>(connection->private_data);
	auto conn = reinterpret_cast<duckdb::Connection *>(conn_wrapper->connection);
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
	auto conn_wrapper = static_cast<duckdb::DuckDBAdbcConnectionWrapper *>(connection->private_data);
	auto conn = reinterpret_cast<duckdb::Connection *>(conn_wrapper->connection);
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
	auto conn_wrapper = static_cast<duckdb::DuckDBAdbcConnectionWrapper *>(connection->private_data);
	conn_wrapper->connection = nullptr;

	auto res = duckdb_connect(database_wrapper->database, &conn_wrapper->connection);
	auto adbc_status = CheckResult(res, error, "Failed to connect to Database");
	if (adbc_status != ADBC_STATUS_OK) {
		return adbc_status;
	}
	// We might have options to set
	auto conn = reinterpret_cast<duckdb::Connection *>(conn_wrapper->connection);
	return InternalSetOption(*conn, conn_wrapper->options, error);
}

AdbcStatusCode ConnectionRelease(struct AdbcConnection *connection, struct AdbcError *error) {
	if (connection && connection->private_data) {
		auto conn_wrapper = static_cast<duckdb::DuckDBAdbcConnectionWrapper *>(connection->private_data);
		auto conn = reinterpret_cast<duckdb::Connection *>(conn_wrapper->connection);
		duckdb_disconnect(reinterpret_cast<duckdb_connection *>(&conn));
		delete conn_wrapper;
		connection->private_data = nullptr;
	}
	return ADBC_STATUS_OK;
}

// some stream callbacks

static int get_schema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
	if (!stream || !stream->private_data || !out) {
		return DuckDBError;
	}
	auto result_wrapper = static_cast<DuckDBAdbcStreamWrapper *>(stream->private_data);
	auto count = duckdb_column_count(&result_wrapper->result);
	std::vector<duckdb_logical_type> types(count);

	std::vector<std::string> owned_names;
	owned_names.reserve(count);
	duckdb::vector<const char *> names(count);
	for (idx_t i = 0; i < count; i++) {
		types[i] = duckdb_column_logical_type(&result_wrapper->result, i);
		auto column_name = duckdb_column_name(&result_wrapper->result, i);
		owned_names.emplace_back(column_name);
		names[i] = owned_names.back().c_str();
	}

	auto arrow_options = duckdb_result_get_arrow_options(&result_wrapper->result);

	auto res = duckdb_to_arrow_schema(arrow_options, types.data(), names.data(), count, out);
	duckdb_destroy_arrow_options(&arrow_options);
	for (auto &type : types) {
		duckdb_destroy_logical_type(&type);
	}
	if (res) {
		duckdb_destroy_error_data(&res);
		return DuckDBError;
	}
	return DuckDBSuccess;
}

static int get_next(struct ArrowArrayStream *stream, struct ArrowArray *out) {
	if (!stream || !stream->private_data || !out) {
		return DuckDBError;
	}
	out->release = nullptr;
	auto result_wrapper = static_cast<DuckDBAdbcStreamWrapper *>(stream->private_data);
	auto duckdb_chunk = duckdb_fetch_chunk(result_wrapper->result);
	if (!duckdb_chunk) {
		return DuckDBSuccess;
	}
	auto arrow_options = duckdb_result_get_arrow_options(&result_wrapper->result);

	auto conversion_success = duckdb_data_chunk_to_arrow(arrow_options, duckdb_chunk, out);
	duckdb_destroy_arrow_options(&arrow_options);
	duckdb_destroy_data_chunk(&duckdb_chunk);

	if (conversion_success) {
		duckdb_destroy_error_data(&conversion_success);
		return DuckDBError;
	}
	return DuckDBSuccess;
}

void release(struct ArrowArrayStream *stream) {
	if (!stream || !stream->release) {
		return;
	}
	auto result_wrapper = reinterpret_cast<DuckDBAdbcStreamWrapper *>(stream->private_data);
	if (result_wrapper) {
		duckdb_destroy_result(&result_wrapper->result);
	}
	free(stream->private_data);
	stream->private_data = nullptr;
	stream->release = nullptr;
}

const char *get_last_error(struct ArrowArrayStream *stream) {
	return nullptr;
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

	duckdb::ArrowSchemaWrapper arrow_schema_wrapper;
	ConvertedSchemaWrapper out_types;

	input->get_schema(input, &arrow_schema_wrapper.arrow_schema);
	auto res = duckdb_schema_from_arrow(connection, &arrow_schema_wrapper.arrow_schema, out_types.GetPtr());
	if (res) {
		SetError(error, duckdb_error_data_message(res));
		duckdb_destroy_error_data(&res);
		return ADBC_STATUS_INTERNAL;
	}

	auto &d_converted_schema = *reinterpret_cast<duckdb::ArrowTableSchema *>(out_types.Get());
	auto types = d_converted_schema.GetTypes();
	auto names = d_converted_schema.GetNames();

	if (ingestion_mode == IngestionMode::CREATE) {
		// We must construct the create table SQL query
		std::ostringstream create_table;
		create_table << "CREATE TABLE ";
		if (schema) {
			create_table << duckdb::KeywordHelper::WriteOptionallyQuoted(schema) << ".";
		}
		create_table << duckdb::KeywordHelper::WriteOptionallyQuoted(table_name) << " (";
		for (idx_t i = 0; i < types.size(); i++) {
			create_table << duckdb::KeywordHelper::WriteOptionallyQuoted(names[i]);
			create_table << " " << types[i].ToString();
			if (i + 1 < types.size()) {
				create_table << ", ";
			}
		}
		create_table << ");";
		duckdb_result result;
		if (duckdb_query(connection, create_table.str().c_str(), &result) == DuckDBError) {
			SetError(error, duckdb_result_error(&result));
			duckdb_destroy_result(&result);
			return ADBC_STATUS_INTERNAL;
		}
		duckdb_destroy_result(&result);
	}
	AppenderWrapper appender(connection, schema, table_name);
	if (!appender.Valid()) {
		return ADBC_STATUS_INTERNAL;
	}
	duckdb::ArrowArrayWrapper arrow_array_wrapper;

	input->get_next(input, &arrow_array_wrapper.arrow_array);
	while (arrow_array_wrapper.arrow_array.release) {
		DataChunkWrapper out_chunk;
		auto res = duckdb_data_chunk_from_arrow(connection, &arrow_array_wrapper.arrow_array, out_types.Get(),
		                                        &out_chunk.chunk);
		if (res) {
			SetError(error, duckdb_error_data_message(res));
			duckdb_destroy_error_data(&res);
		}
		if (duckdb_append_data_chunk(appender.Get(), out_chunk.chunk) != DuckDBSuccess) {
			return ADBC_STATUS_INTERNAL;
		}
		arrow_array_wrapper = duckdb::ArrowArrayWrapper();
		input->get_next(input, &arrow_array_wrapper.arrow_array);
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
	auto conn_wrapper = static_cast<duckdb::DuckDBAdbcConnectionWrapper *>(connection->private_data);

	statement_wrapper->connection = conn_wrapper->connection;
	statement_wrapper->statement = nullptr;
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

	auto prepared_wrapper = reinterpret_cast<duckdb::PreparedStatementWrapper *>(wrapper->statement);
	if (!prepared_wrapper || !prepared_wrapper->statement || !prepared_wrapper->statement->data) {
		SetError(error, "Invalid prepared statement wrapper");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	auto count = prepared_wrapper->statement->data->properties.parameter_count;
	std::vector<duckdb_logical_type> types(count);
	std::vector<std::string> owned_names;
	owned_names.reserve(count);
	duckdb::vector<const char *> names(count);

	for (idx_t i = 0; i < count; i++) {
		// FIXME: we don't support named parameters yet, but when we do, this needs to be updated
		// Every prepared parameter type is UNKNOWN, which we need to map to NULL according to the spec of
		// 'AdbcStatementGetParameterSchema'
		types[i] = duckdb_create_logical_type(DUCKDB_TYPE_SQLNULL);
		auto column_name = std::to_string(i);
		owned_names.emplace_back(column_name);
		names[i] = owned_names.back().c_str();
	}

	duckdb_arrow_options arrow_options;
	duckdb_connection_get_arrow_options(wrapper->connection, &arrow_options);

	auto res = duckdb_to_arrow_schema(arrow_options, types.data(), names.data(), count, schema);

	for (auto &type : types) {
		duckdb_destroy_logical_type(&type);
	}
	duckdb_destroy_arrow_options(&arrow_options);

	if (res) {
		SetError(error, duckdb_error_data_message(res));
		duckdb_destroy_error_data(&res);
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	return ADBC_STATUS_OK;
}

static AdbcStatusCode IngestToTableFromBoundStream(DuckDBAdbcStatementWrapper *statement, AdbcError *error) {
	// See ADBC_INGEST_OPTION_TARGET_TABLE
	D_ASSERT(statement->ingestion_stream.release);
	D_ASSERT(statement->ingestion_table_name);

	// Take the input stream from the statement
	auto stream = statement->ingestion_stream;

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
	auto stream_wrapper = static_cast<DuckDBAdbcStreamWrapper *>(malloc(sizeof(DuckDBAdbcStreamWrapper)));
	// Only process the stream if there are parameters to bind
	auto prepared_statement_params = reinterpret_cast<duckdb::PreparedStatementWrapper *>(wrapper->statement)
	                                     ->statement->data->properties.parameter_count;
	if (has_stream && prepared_statement_params > 0) {
		// A stream was bound to the statement, use that to bind parameters
		ArrowArrayStream stream = wrapper->ingestion_stream;
		ConvertedSchemaWrapper out_types;
		duckdb::ArrowSchemaWrapper arrow_schema_wrapper;
		stream.get_schema(&stream, &arrow_schema_wrapper.arrow_schema);
		try {
			auto res =
			    duckdb_schema_from_arrow(wrapper->connection, &arrow_schema_wrapper.arrow_schema, out_types.GetPtr());
			if (res) {
				SetError(error, duckdb_error_data_message(res));
				duckdb_destroy_error_data(&res);
			}
		} catch (...) {
			free(stream_wrapper);
			return ADBC_STATUS_INTERNAL;
		}

		duckdb::ArrowArrayWrapper arrow_array_wrapper;

		stream.get_next(&stream, &arrow_array_wrapper.arrow_array);

		while (arrow_array_wrapper.arrow_array.release) {
			// This is a valid arrow array, let's make it into a data chunk
			DataChunkWrapper out_chunk;
			auto res_conv = duckdb_data_chunk_from_arrow(wrapper->connection, &arrow_array_wrapper.arrow_array,
			                                             out_types.Get(), &out_chunk.chunk);
			if (res_conv) {
				SetError(error, duckdb_error_data_message(res_conv));
				duckdb_destroy_error_data(&res_conv);
				return ADBC_STATUS_INVALID_ARGUMENT;
			}
			if (!out_chunk.chunk) {
				SetError(error, "Please provide a non-empty chunk to be bound");
				free(stream_wrapper);
				return ADBC_STATUS_INVALID_ARGUMENT;
			}
			auto chunk = reinterpret_cast<duckdb::DataChunk *>(out_chunk.chunk);
			if (chunk->size() == 0) {
				SetError(error, "Please provide a non-empty chunk to be bound");
				free(stream_wrapper);
				return ADBC_STATUS_INVALID_ARGUMENT;
			}
			if (chunk->size() != 1) {
				// TODO: add support for binding multiple rows
				SetError(error, "Binding multiple rows at once is not supported yet");
				free(stream_wrapper);
				return ADBC_STATUS_NOT_IMPLEMENTED;
			}
			if (chunk->ColumnCount() > prepared_statement_params) {
				SetError(error, "Input data has more column than prepared statement has parameters");
				free(stream_wrapper);
				return ADBC_STATUS_INVALID_ARGUMENT;
			}
			duckdb_clear_bindings(wrapper->statement);
			for (idx_t col_idx = 0; col_idx < chunk->ColumnCount(); col_idx++) {
				auto val = chunk->GetValue(col_idx, 0);
				auto duck_val = reinterpret_cast<duckdb_value>(&val);
				auto res = duckdb_bind_value(wrapper->statement, 1 + col_idx, duck_val);
				if (res != DuckDBSuccess) {
					SetError(error, duckdb_prepare_error(wrapper->statement));
					free(stream_wrapper);
					return ADBC_STATUS_INVALID_ARGUMENT;
				}
			}
			auto res = duckdb_execute_prepared(wrapper->statement, &stream_wrapper->result);
			if (res != DuckDBSuccess) {
				SetError(error, duckdb_result_error(&stream_wrapper->result));
				free(stream_wrapper);
				return ADBC_STATUS_INVALID_ARGUMENT;
			}
			// Recreate wrappers for next iteration
			arrow_array_wrapper = duckdb::ArrowArrayWrapper();
			stream.get_next(&stream, &arrow_array_wrapper.arrow_array);
		}
	} else {
		auto res = duckdb_execute_prepared(wrapper->statement, &stream_wrapper->result);
		if (res != DuckDBSuccess) {
			SetError(error, duckdb_result_error(&stream_wrapper->result));
			return ADBC_STATUS_INVALID_ARGUMENT;
		}
	}

	if (out) {
		// We pass ownership of the statement private data to our stream
		out->private_data = stream_wrapper;
		out->get_schema = get_schema;
		out->get_next = get_next;
		out->release = release;
		out->get_last_error = get_last_error;
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
		SetError(error, error_msg_extract_statements);
		duckdb_destroy_extracted(&extracted_statements);
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
	duckdb::stringstream ss;
	ss << "Statement Set Option " << key << " is not yet accepted by DuckDB";
	SetError(error, ss.str());
	return ADBC_STATUS_INVALID_ARGUMENT;
}

AdbcStatusCode ConnectionGetObjects(struct AdbcConnection *connection, int depth, const char *catalog,
                                    const char *db_schema, const char *table_name, const char **table_type,
                                    const char *column_name, struct ArrowArrayStream *out, struct AdbcError *error) {
	std::string catalog_filter = catalog ? catalog : "%";
	std::string db_schema_filter = db_schema ? db_schema : "%";
	std::string table_name_filter = table_name ? table_name : "%";
	std::string table_type_condition = "";
	if (table_type && table_type[0]) {
		table_type_condition = " AND table_type IN (";
		for (int i = 0; table_type[i]; ++i) {
			if ((strcmp(table_type[i], "LOCAL TABLE") != 0) && (strcmp(table_type[i], "BASE TABLE") != 0) &&
			    (strcmp(table_type[i], "VIEW") != 0)) {
				duckdb::stringstream ss;
				ss << "Table type must be \"LOCAL TABLE\", \"BASE TABLE\" or "
				   << "\"VIEW\": \"" << table_type[i] << "\"";
				SetError(error, ss.str());
				return ADBC_STATUS_INVALID_ARGUMENT;
			}
			if (i > 0) {
				table_type_condition += ", ";
			}
			table_type_condition += "'";
			table_type_condition += table_type[i];
			table_type_condition += "'";
		}
		table_type_condition += ")";
	}
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
					WHERE table_name LIKE '%s'%s
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
		                                   table_name_filter, table_type_condition, db_schema_filter, catalog_filter);
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
							remarks: '',
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
						database_name AS table_catalog,
						schema_name AS table_schema,
						table_name,
						LIST({
							constraint_name: constraint_name,
							constraint_type: constraint_type,
							constraint_column_names: constraint_column_names,
							constraint_column_usage: list_transform(
								referenced_column_names,
								lambda name: {
									fk_catalog: database_name,
									fk_db_schema: schema_name,
									fk_table: referenced_table,
									fk_column_name: name,
								}
							)
						}) table_constraints
					FROM duckdb_constraints()
					WHERE
						constraint_type NOT IN ('NOT NULL') AND
						list_has_any(
							constraint_column_names,
							list_filter(
								constraint_column_names,
								lambda name: name LIKE '%s'
							)
						)
					GROUP BY database_name, schema_name, table_name
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
					WHERE table_name LIKE '%s'%s
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
		                                   column_name_filter, column_name_filter, table_name_filter,
		                                   table_type_condition, db_schema_filter, catalog_filter);
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
