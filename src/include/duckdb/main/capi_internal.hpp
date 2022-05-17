//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/capi_internal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.h"
#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/appender.hpp"
#include <cstring>
#include <cassert>

#ifdef _WIN32
#ifndef strdup
#define strdup _strdup
#endif
#endif

namespace duckdb {

struct DatabaseData {
	unique_ptr<DuckDB> database;
};

struct PreparedStatementWrapper {
	unique_ptr<PreparedStatement> statement;
	vector<Value> values;
};

struct ArrowResultWrapper {
	unique_ptr<MaterializedQueryResult> result;
	unique_ptr<DataChunk> current_chunk;
	string timezone_config;
};

struct AppenderWrapper {
	unique_ptr<Appender> appender;
	string error;
};

enum class CAPIResultSetType : uint8_t {
	CAPI_RESULT_TYPE_NONE = 0,
	CAPI_RESULT_TYPE_MATERIALIZED,
	CAPI_RESULT_TYPE_DEPRECATED
};

struct DuckDBResultData {
	//! The underlying query result
	unique_ptr<QueryResult> result;
	// Results can only use either the new API or the old API, not a mix of the two
	// They start off as "none" and switch to one or the other when an API method is used
	CAPIResultSetType result_set_type;
};

duckdb_type ConvertCPPTypeToC(const LogicalType &type);
LogicalTypeId ConvertCTypeToCPP(duckdb_type c_type);
idx_t GetCTypeSize(duckdb_type type);
duckdb_state duckdb_translate_result(unique_ptr<QueryResult> result, duckdb_result *out);
bool deprecated_materialize_result(duckdb_result *result);

} // namespace duckdb
