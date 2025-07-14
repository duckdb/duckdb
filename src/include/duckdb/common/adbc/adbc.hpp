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

// RAII wrappers for C-API resources used in Ingest
class ArrowSchemaWrapper {
public:
	ArrowSchemaWrapper() : schema(new ArrowSchema) {
	}
	~ArrowSchemaWrapper() {
		if (schema) {
			if (schema->release) {
				schema->release(schema);
			}
			delete schema;
		}
	}
	ArrowSchema *get() const {
		return schema;
	}
	ArrowSchema &operator*() const {
		return *schema;
	}
	ArrowSchema *operator->() const {
		return schema;
	}
	ArrowSchemaWrapper(const ArrowSchemaWrapper &) = delete;
	ArrowSchemaWrapper &operator=(const ArrowSchemaWrapper &) = delete;
	ArrowSchemaWrapper(ArrowSchemaWrapper &&other) noexcept : schema(other.schema) {
		other.schema = nullptr;
	}
	ArrowSchemaWrapper &operator=(ArrowSchemaWrapper &&other) noexcept {
		if (this != &other) {
			delete schema;
			schema = other.schema;
			other.schema = nullptr;
		}
		return *this;
	}

private:
	ArrowSchema *schema;
};

class ArrowArrayWrapper {
public:
	ArrowArrayWrapper() : array(new ArrowArray) {
	}
	~ArrowArrayWrapper() {
		if (array) {
			if (array->release) {
				array->release(array);
			}
			delete array;
		}
	}
	ArrowArray *get() const {
		return array;
	}
	ArrowArray &operator*() const {
		return *array;
	}
	ArrowArray *operator->() const {
		return array;
	}
	ArrowArrayWrapper(const ArrowArrayWrapper &) = delete;
	ArrowArrayWrapper &operator=(const ArrowArrayWrapper &) = delete;
	ArrowArrayWrapper(ArrowArrayWrapper &&other) noexcept : array(other.array) {
		other.array = nullptr;
	}
	ArrowArrayWrapper &operator=(ArrowArrayWrapper &&other) noexcept {
		if (this != &other) {
			if (array && array->release) {
				array->release(array);
			}
			delete array;
			array = other.array;
			other.array = nullptr;
		}
		return *this;
	}

private:
	ArrowArray *array;
};

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

	duckdb_appender get() const {
		return appender;
	}
	explicit operator duckdb_appender() const {
		return appender;
	}
	bool valid() const {
		return appender != nullptr;
	}
	AppenderWrapper(const AppenderWrapper &) = delete;
	AppenderWrapper &operator=(const AppenderWrapper &) = delete;
	AppenderWrapper(AppenderWrapper &&other) noexcept : appender(other.appender) {
		other.appender = nullptr;
	}
	AppenderWrapper &operator=(AppenderWrapper &&other) noexcept {
		if (this != &other) {
			if (appender) {
				duckdb_appender_destroy(&appender);
			}
			appender = other.appender;
			other.appender = nullptr;
		}
		return *this;
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

	DataChunkWrapper(const DataChunkWrapper &) = delete;
	DataChunkWrapper &operator=(const DataChunkWrapper &) = delete;
	DataChunkWrapper(DataChunkWrapper &&other) noexcept : chunk(other.chunk) {
		other.chunk = nullptr;
	}
	DataChunkWrapper &operator=(DataChunkWrapper &&other) noexcept {
		if (this != &other) {
			if (chunk) {
				duckdb_destroy_data_chunk(&chunk);
			}
			chunk = other.chunk;
			other.chunk = nullptr;
		}
		return *this;
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
	duckdb_arrow_converted_schema *get_ptr() {
		return &schema;
	}

	explicit operator duckdb_arrow_converted_schema() const {
		return schema;
	}
	duckdb_arrow_converted_schema get() const {
		return schema;
	}
	void set(duckdb_arrow_converted_schema s) {
		schema = s;
	}
	ConvertedSchemaWrapper(const ConvertedSchemaWrapper &) = delete;
	ConvertedSchemaWrapper &operator=(const ConvertedSchemaWrapper &) = delete;
	ConvertedSchemaWrapper(ConvertedSchemaWrapper &&other) noexcept : schema(other.schema) {
		other.schema = nullptr;
	}
	ConvertedSchemaWrapper &operator=(ConvertedSchemaWrapper &&other) noexcept {
		if (this != &other) {
			if (schema) {
				duckdb_destroy_arrow_converted_schema(&schema);
			}
			schema = other.schema;
			other.schema = nullptr;
		}
		return *this;
	}

private:
	duckdb_arrow_converted_schema schema;
};
class OutNamesWrapper {
public:
	OutNamesWrapper(char **names, idx_t count) : names(names), count(count) {
	}
	~OutNamesWrapper() {
		if (names) {
			for (idx_t i = 0; i < count; i++) {
				delete[] names[i];
			}
			delete[] names;
		}
	}
	char **get() const {
		return names;
	}
	OutNamesWrapper(const OutNamesWrapper &) = delete;
	OutNamesWrapper &operator=(const OutNamesWrapper &) = delete;
	OutNamesWrapper(OutNamesWrapper &&other) noexcept : names(other.names), count(other.count) {
		other.names = nullptr;
		other.count = 0;
	}
	OutNamesWrapper &operator=(OutNamesWrapper &&other) noexcept {
		if (this != &other) {
			if (names) {
				for (idx_t i = 0; i < count; i++) {
					delete[] names[i];
				}
				delete[] names;
			}
			names = other.names;
			count = other.count;
			other.names = nullptr;
			other.count = 0;
		}
		return *this;
	}
	void SetColumnCount(idx_t count_p) {
		count = count_p;
	}

private:
	char **names;
	idx_t count = 0;
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
// void SetError(struct AdbcError *error, const char *message);
