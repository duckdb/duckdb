//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/capi/capi_internal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.h"
#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

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
	//! Map of name -> values
	case_insensitive_map_t<Value> values;
	unique_ptr<PreparedStatement> statement;
};

struct ExtractStatementsWrapper {
	vector<unique_ptr<SQLStatement>> statements;
	string error;
};

struct PendingStatementWrapper {
	unique_ptr<PendingQueryResult> statement;
	bool allow_streaming;
};

struct ArrowResultWrapper {
	unique_ptr<MaterializedQueryResult> result;
	unique_ptr<DataChunk> current_chunk;
	ClientProperties options;
};

struct AppenderWrapper {
	unique_ptr<Appender> appender;
	string error;
};

enum class CAPIResultSetType : uint8_t {
	CAPI_RESULT_TYPE_NONE = 0,
	CAPI_RESULT_TYPE_MATERIALIZED,
	CAPI_RESULT_TYPE_STREAMING,
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
