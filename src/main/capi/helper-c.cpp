#include "duckdb/main/capi/capi_internal.hpp"

namespace duckdb {

LogicalTypeId ConvertCTypeToCPP(duckdb_type c_type) {
	switch (c_type) {
	case DUCKDB_TYPE_BOOLEAN:
		return LogicalTypeId::BOOLEAN;
	case DUCKDB_TYPE_TINYINT:
		return LogicalTypeId::TINYINT;
	case DUCKDB_TYPE_SMALLINT:
		return LogicalTypeId::SMALLINT;
	case DUCKDB_TYPE_INTEGER:
		return LogicalTypeId::INTEGER;
	case DUCKDB_TYPE_BIGINT:
		return LogicalTypeId::BIGINT;
	case DUCKDB_TYPE_UTINYINT:
		return LogicalTypeId::UTINYINT;
	case DUCKDB_TYPE_USMALLINT:
		return LogicalTypeId::USMALLINT;
	case DUCKDB_TYPE_UINTEGER:
		return LogicalTypeId::UINTEGER;
	case DUCKDB_TYPE_UBIGINT:
		return LogicalTypeId::UBIGINT;
	case DUCKDB_TYPE_HUGEINT:
		return LogicalTypeId::HUGEINT;
	case DUCKDB_TYPE_FLOAT:
		return LogicalTypeId::FLOAT;
	case DUCKDB_TYPE_DOUBLE:
		return LogicalTypeId::DOUBLE;
	case DUCKDB_TYPE_TIMESTAMP:
		return LogicalTypeId::TIMESTAMP;
	case DUCKDB_TYPE_DATE:
		return LogicalTypeId::DATE;
	case DUCKDB_TYPE_TIME:
		return LogicalTypeId::TIME;
	case DUCKDB_TYPE_VARCHAR:
		return LogicalTypeId::VARCHAR;
	case DUCKDB_TYPE_BLOB:
		return LogicalTypeId::BLOB;
	case DUCKDB_TYPE_INTERVAL:
		return LogicalTypeId::INTERVAL;
	case DUCKDB_TYPE_TIMESTAMP_S:
		return LogicalTypeId::TIMESTAMP_SEC;
	case DUCKDB_TYPE_TIMESTAMP_MS:
		return LogicalTypeId::TIMESTAMP_MS;
	case DUCKDB_TYPE_TIMESTAMP_NS:
		return LogicalTypeId::TIMESTAMP_NS;
	case DUCKDB_TYPE_UUID:
		return LogicalTypeId::UUID;
	default: // LCOV_EXCL_START
		D_ASSERT(0);
		return LogicalTypeId::INVALID;
	} // LCOV_EXCL_STOP
}

duckdb_type ConvertCPPTypeToC(const LogicalType &sql_type) {
	switch (sql_type.id()) {
	case LogicalTypeId::BOOLEAN:
		return DUCKDB_TYPE_BOOLEAN;
	case LogicalTypeId::TINYINT:
		return DUCKDB_TYPE_TINYINT;
	case LogicalTypeId::SMALLINT:
		return DUCKDB_TYPE_SMALLINT;
	case LogicalTypeId::INTEGER:
		return DUCKDB_TYPE_INTEGER;
	case LogicalTypeId::BIGINT:
		return DUCKDB_TYPE_BIGINT;
	case LogicalTypeId::UTINYINT:
		return DUCKDB_TYPE_UTINYINT;
	case LogicalTypeId::USMALLINT:
		return DUCKDB_TYPE_USMALLINT;
	case LogicalTypeId::UINTEGER:
		return DUCKDB_TYPE_UINTEGER;
	case LogicalTypeId::UBIGINT:
		return DUCKDB_TYPE_UBIGINT;
	case LogicalTypeId::HUGEINT:
		return DUCKDB_TYPE_HUGEINT;
	case LogicalTypeId::FLOAT:
		return DUCKDB_TYPE_FLOAT;
	case LogicalTypeId::DOUBLE:
		return DUCKDB_TYPE_DOUBLE;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return DUCKDB_TYPE_TIMESTAMP;
	case LogicalTypeId::TIMESTAMP_SEC:
		return DUCKDB_TYPE_TIMESTAMP_S;
	case LogicalTypeId::TIMESTAMP_MS:
		return DUCKDB_TYPE_TIMESTAMP_MS;
	case LogicalTypeId::TIMESTAMP_NS:
		return DUCKDB_TYPE_TIMESTAMP_NS;
	case LogicalTypeId::DATE:
		return DUCKDB_TYPE_DATE;
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return DUCKDB_TYPE_TIME;
	case LogicalTypeId::VARCHAR:
		return DUCKDB_TYPE_VARCHAR;
	case LogicalTypeId::BLOB:
		return DUCKDB_TYPE_BLOB;
	case LogicalTypeId::BIT:
		return DUCKDB_TYPE_BIT;
	case LogicalTypeId::INTERVAL:
		return DUCKDB_TYPE_INTERVAL;
	case LogicalTypeId::DECIMAL:
		return DUCKDB_TYPE_DECIMAL;
	case LogicalTypeId::ENUM:
		return DUCKDB_TYPE_ENUM;
	case LogicalTypeId::LIST:
		return DUCKDB_TYPE_LIST;
	case LogicalTypeId::STRUCT:
		return DUCKDB_TYPE_STRUCT;
	case LogicalTypeId::MAP:
		return DUCKDB_TYPE_MAP;
	case LogicalTypeId::UNION:
		return DUCKDB_TYPE_UNION;
	case LogicalTypeId::UUID:
		return DUCKDB_TYPE_UUID;
	default: // LCOV_EXCL_START
		D_ASSERT(0);
		return DUCKDB_TYPE_INVALID;
	} // LCOV_EXCL_STOP
}

idx_t GetCTypeSize(duckdb_type type) {
	switch (type) {
	case DUCKDB_TYPE_BOOLEAN:
		return sizeof(bool);
	case DUCKDB_TYPE_TINYINT:
		return sizeof(int8_t);
	case DUCKDB_TYPE_SMALLINT:
		return sizeof(int16_t);
	case DUCKDB_TYPE_INTEGER:
		return sizeof(int32_t);
	case DUCKDB_TYPE_BIGINT:
		return sizeof(int64_t);
	case DUCKDB_TYPE_UTINYINT:
		return sizeof(uint8_t);
	case DUCKDB_TYPE_USMALLINT:
		return sizeof(uint16_t);
	case DUCKDB_TYPE_UINTEGER:
		return sizeof(uint32_t);
	case DUCKDB_TYPE_UBIGINT:
		return sizeof(uint64_t);
	case DUCKDB_TYPE_HUGEINT:
	case DUCKDB_TYPE_UUID:
		return sizeof(duckdb_hugeint);
	case DUCKDB_TYPE_FLOAT:
		return sizeof(float);
	case DUCKDB_TYPE_DOUBLE:
		return sizeof(double);
	case DUCKDB_TYPE_DATE:
		return sizeof(duckdb_date);
	case DUCKDB_TYPE_TIME:
		return sizeof(duckdb_time);
	case DUCKDB_TYPE_TIMESTAMP:
	case DUCKDB_TYPE_TIMESTAMP_S:
	case DUCKDB_TYPE_TIMESTAMP_MS:
	case DUCKDB_TYPE_TIMESTAMP_NS:
		return sizeof(duckdb_timestamp);
	case DUCKDB_TYPE_VARCHAR:
		return sizeof(const char *);
	case DUCKDB_TYPE_BLOB:
		return sizeof(duckdb_blob);
	case DUCKDB_TYPE_INTERVAL:
		return sizeof(duckdb_interval);
	case DUCKDB_TYPE_DECIMAL:
		return sizeof(duckdb_hugeint);
	default: // LCOV_EXCL_START
		// unsupported type
		D_ASSERT(0);
		return sizeof(const char *);
	} // LCOV_EXCL_STOP
}

duckdb_statement_type StatementTypeToC(duckdb::StatementType statement_type) {
	switch (statement_type) {
	case duckdb::StatementType::SELECT_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_SELECT;
	case duckdb::StatementType::INVALID_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_INVALID;
	case duckdb::StatementType::INSERT_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_INSERT;
	case duckdb::StatementType::UPDATE_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_UPDATE;
	case duckdb::StatementType::EXPLAIN_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_EXPLAIN;
	case duckdb::StatementType::DELETE_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_DELETE;
	case duckdb::StatementType::PREPARE_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_PREPARE;
	case duckdb::StatementType::CREATE_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_CREATE;
	case duckdb::StatementType::EXECUTE_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_EXECUTE;
	case duckdb::StatementType::ALTER_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_ALTER;
	case duckdb::StatementType::TRANSACTION_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_TRANSACTION;
	case duckdb::StatementType::COPY_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_COPY;
	case duckdb::StatementType::ANALYZE_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_ANALYZE;
	case duckdb::StatementType::VARIABLE_SET_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_VARIABLE_SET;
	case duckdb::StatementType::CREATE_FUNC_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_CREATE_FUNC;
	case duckdb::StatementType::DROP_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_DROP;
	case duckdb::StatementType::EXPORT_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_EXPORT;
	case duckdb::StatementType::PRAGMA_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_PRAGMA;
	case duckdb::StatementType::SHOW_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_SHOW;
	case duckdb::StatementType::VACUUM_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_VACUUM;
	case duckdb::StatementType::CALL_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_CALL;
	case duckdb::StatementType::SET_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_SET;
	case duckdb::StatementType::LOAD_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_LOAD;
	case duckdb::StatementType::RELATION_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_RELATION;
	case duckdb::StatementType::EXTENSION_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_EXTENSION;
	case duckdb::StatementType::LOGICAL_PLAN_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_LOGICAL_PLAN;
	case duckdb::StatementType::ATTACH_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_ATTACH;
	case duckdb::StatementType::DETACH_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_DETACH;
	case duckdb::StatementType::MULTI_STATEMENT:
		return DUCKDB_STATEMENT_TYPE_MULTI;
	default:
		return DUCKDB_STATEMENT_TYPE_INVALID;
	}
}

} // namespace duckdb

void *duckdb_malloc(size_t size) {
	return malloc(size);
}

void duckdb_free(void *ptr) {
	free(ptr);
}

idx_t duckdb_vector_size() {
	return STANDARD_VECTOR_SIZE;
}

bool duckdb_string_is_inlined(duckdb_string_t string_p) {
	static_assert(sizeof(duckdb_string_t) == sizeof(duckdb::string_t),
	              "duckdb_string_t should have the same memory layout as duckdb::string_t");
	auto &string = *(duckdb::string_t *)(&string_p);
	return string.IsInlined();
}
