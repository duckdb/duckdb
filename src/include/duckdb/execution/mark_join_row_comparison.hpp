//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/mark_join_row_comparison.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"

namespace duckdb {

class DataChunk;
struct JoinCondition;

struct MarkJoinRowComparison {
	static void UpdateRangeBound(const Vector &key, ExpressionType comparison, Value &bound, idx_t &null_count);
	static void Compare(const Vector &left, const Vector &right, ExpressionType comparison_type, Vector &result);
	static void Compare(const Vector &left, idx_t left_row, const Vector &right, ExpressionType comparison_type,
	                    Vector &result);
	static void CompareConjunction(DataChunk &left, idx_t left_row, DataChunk &right,
	                               const vector<JoinCondition> &conditions, Vector &result);
	static void Perform(DataChunk &left, DataChunk &right, bool found_match[], const vector<JoinCondition> &conditions,
	                    optional_ptr<bool> found_unknown);
};

} // namespace duckdb
