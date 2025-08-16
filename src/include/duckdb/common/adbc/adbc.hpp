//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/adbc/adbc.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/adbc/adbc.h"

#include "duckdb/main/capi/capi_internal.hpp"

#include <string>

namespace duckdb_adbc {

class AppenderWrapper {
public:
	AppenderWrapper(duckdb_connection conn, const char *schema, const char *table) : appender(nullptr) {
		if (duckdb_appender_create(conn, schema, table, &appender) != DuckDBSuccess) {
			appender = nullptr;
		}
	}
	~AppenderWrapper() {
		if (appender) {
			duckdb_appender_destroy(&appender);
		}
	}

	duckdb_appender Get() const {
		return appender;
	}
	bool Valid() const {
		return appender != nullptr;
	}

private:
	duckdb_appender appender;
};

class DataChunkWrapper {
public:
	DataChunkWrapper() : chunk(nullptr) {
	}

	~DataChunkWrapper() {
		if (chunk) {
			duckdb_destroy_data_chunk(&chunk);
		}
	}

	explicit operator duckdb_data_chunk() const {
		return chunk;
	}

	duckdb_data_chunk chunk;
};

class ConvertedSchemaWrapper {
public:
	ConvertedSchemaWrapper() : schema(nullptr) {
	}
	~ConvertedSchemaWrapper() {
		if (schema) {
			duckdb_destroy_arrow_converted_schema(&schema);
		}
	}
	duckdb_arrow_converted_schema *GetPtr() {
		return &schema;
	}

	explicit operator duckdb_arrow_converted_schema() const {
		return schema;
	}
	duckdb_arrow_converted_schema Get() const {
		return schema;
	}

private:
	duckdb_arrow_converted_schema schema;
};

AdbcStatusCode DatabaseNew(struct AdbcDatabase *database, struct AdbcError *error);

AdbcStatusCode DatabaseSetOption(struct AdbcDatabase *database, const char *key, const char *value,
                                 struct AdbcError *error);

AdbcStatusCode DatabaseInit(struct AdbcDatabase *database, struct AdbcError *error);

AdbcStatusCode DatabaseRelease(struct AdbcDatabase *database, struct AdbcError *error);

AdbcStatusCode ConnectionNew(struct AdbcConnection *connection, struct AdbcError *error);

AdbcStatusCode ConnectionSetOption(struct AdbcConnection *connection, const char *key, const char *value,
                                   struct AdbcError *error);

AdbcStatusCode ConnectionInit(struct AdbcConnection *connection, struct AdbcDatabase *database,
                              struct AdbcError *error);

AdbcStatusCode ConnectionRelease(struct AdbcConnection *connection, struct AdbcError *error);

AdbcStatusCode ConnectionGetInfo(struct AdbcConnection *connection, const uint32_t *info_codes,
                                 size_t info_codes_length, struct ArrowArrayStream *out, struct AdbcError *error);

AdbcStatusCode ConnectionGetObjects(struct AdbcConnection *connection, int depth, const char *catalog,
                                    const char *db_schema, const char *table_name, const char **table_type,
                                    const char *column_name, struct ArrowArrayStream *out, struct AdbcError *error);

AdbcStatusCode ConnectionGetTableSchema(struct AdbcConnection *connection, const char *catalog, const char *db_schema,
                                        const char *table_name, struct ArrowSchema *schema, struct AdbcError *error);

AdbcStatusCode ConnectionGetTableTypes(struct AdbcConnection *connection, struct ArrowArrayStream *out,
                                       struct AdbcError *error);

AdbcStatusCode ConnectionReadPartition(struct AdbcConnection *connection, const uint8_t *serialized_partition,
                                       size_t serialized_length, struct ArrowArrayStream *out, struct AdbcError *error);

AdbcStatusCode ConnectionCommit(struct AdbcConnection *connection, struct AdbcError *error);

AdbcStatusCode ConnectionRollback(struct AdbcConnection *connection, struct AdbcError *error);

AdbcStatusCode StatementNew(struct AdbcConnection *connection, struct AdbcStatement *statement,
                            struct AdbcError *error);

AdbcStatusCode StatementRelease(struct AdbcStatement *statement, struct AdbcError *error);

AdbcStatusCode StatementExecuteQuery(struct AdbcStatement *statement, struct ArrowArrayStream *out,
                                     int64_t *rows_affected, struct AdbcError *error);

AdbcStatusCode StatementPrepare(struct AdbcStatement *statement, struct AdbcError *error);

AdbcStatusCode StatementSetSqlQuery(struct AdbcStatement *statement, const char *query, struct AdbcError *error);

AdbcStatusCode StatementSetSubstraitPlan(struct AdbcStatement *statement, const uint8_t *plan, size_t length,
                                         struct AdbcError *error);

AdbcStatusCode StatementBind(struct AdbcStatement *statement, struct ArrowArray *values, struct ArrowSchema *schema,
                             struct AdbcError *error);

AdbcStatusCode StatementBindStream(struct AdbcStatement *statement, struct ArrowArrayStream *stream,
                                   struct AdbcError *error);

AdbcStatusCode StatementGetParameterSchema(struct AdbcStatement *statement, struct ArrowSchema *schema,
                                           struct AdbcError *error);

AdbcStatusCode StatementSetOption(struct AdbcStatement *statement, const char *key, const char *value,
                                  struct AdbcError *error);

AdbcStatusCode StatementExecutePartitions(struct AdbcStatement *statement, struct ArrowSchema *schema,
                                          struct AdbcPartitions *partitions, int64_t *rows_affected,
                                          struct AdbcError *error);

void InitializeADBCError(AdbcError *error);

} // namespace duckdb_adbc

//! This method should only be called when the string is guaranteed to not be NULL
void SetError(struct AdbcError *error, const std::string &message);
