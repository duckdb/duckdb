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
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_parameter_data.hpp"
#include "duckdb/main/db_instance_cache.hpp"

#include <cstring>
#include <cassert>

#ifdef _WIN32
#ifndef strdup
#define strdup _strdup
#endif
#endif

namespace duckdb {

struct DBInstanceCacheWrapper {
	unique_ptr<DBInstanceCache> instance_cache;
};

struct DatabaseWrapper {
	shared_ptr<DuckDB> database;
};

struct CClientContextWrapper {
	explicit CClientContextWrapper(ClientContext &context) : context(context) {};
	ClientContext &context;
};

struct CClientArrowOptionsWrapper {
	explicit CClientArrowOptionsWrapper(ClientProperties &properties) : properties(properties) {};
	ClientProperties properties;
};

struct PreparedStatementWrapper {
	//! Map of name -> values
	case_insensitive_map_t<BoundParameterData> values;
	unique_ptr<PreparedStatement> statement;
	bool success = true;
	ErrorData error_data;
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
};

struct AppenderWrapper {
	unique_ptr<BaseAppender> appender;
	ErrorData error_data;
};

struct TableDescriptionWrapper {
	unique_ptr<TableDescription> description;
	string error;
};

struct ErrorDataWrapper {
	ErrorData error_data;
};

struct ExpressionWrapper {
	unique_ptr<Expression> expr;
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

duckdb_type LogicalTypeIdToC(const LogicalTypeId type);
LogicalTypeId LogicalTypeIdFromC(const duckdb_type type);
idx_t GetCTypeSize(const duckdb_type type);
duckdb_statement_type StatementTypeToC(const StatementType type);
duckdb_error_type ErrorTypeToC(const ExceptionType type);
ExceptionType ErrorTypeFromC(const duckdb_error_type type);

duckdb_state DuckDBTranslateResult(unique_ptr<QueryResult> result, duckdb_result *out);
bool DeprecatedMaterializeResult(duckdb_result *result);

} // namespace duckdb
