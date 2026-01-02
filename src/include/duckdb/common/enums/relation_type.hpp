//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/relation_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Catalog Types
//===--------------------------------------------------------------------===//
enum class RelationType : uint8_t {
	INVALID_RELATION,
	TABLE_RELATION,
	PROJECTION_RELATION,
	FILTER_RELATION,
	EXPLAIN_RELATION,
	CROSS_PRODUCT_RELATION,
	JOIN_RELATION,
	AGGREGATE_RELATION,
	SET_OPERATION_RELATION,
	DISTINCT_RELATION,
	LIMIT_RELATION,
	ORDER_RELATION,
	CREATE_VIEW_RELATION,
	CREATE_TABLE_RELATION,
	INSERT_RELATION,
	VALUE_LIST_RELATION,
	MATERIALIZED_RELATION,
	DELETE_RELATION,
	UPDATE_RELATION,
	WRITE_CSV_RELATION,
	WRITE_PARQUET_RELATION,
	READ_CSV_RELATION,
	SUBQUERY_RELATION,
	TABLE_FUNCTION_RELATION,
	VIEW_RELATION,
	QUERY_RELATION,
	DELIM_JOIN_RELATION,
	DELIM_GET_RELATION,
	EXTENSION_RELATION = 255
};

string RelationTypeToString(RelationType type);

} // namespace duckdb
