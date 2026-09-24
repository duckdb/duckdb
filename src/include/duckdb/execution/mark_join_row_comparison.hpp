//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/mark_join_row_comparison.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

struct JoinCondition;

struct MarkJoinRowComparison {
	explicit MarkJoinRowComparison(const DataChunk &left);

	static void Compare(const Vector &left, const Vector &right, ExpressionType comparison_type, Vector &result);
	void CompareConjunction(DataChunk &left, idx_t left_row, DataChunk &right, const vector<JoinCondition> &conditions,
	                        Vector &result);
	static void Perform(DataChunk &left, DataChunk &right, bool found_match[], const vector<JoinCondition> &conditions,
	                    optional_ptr<bool> found_unknown);
	static void CompareEquality(const Vector &left, idx_t left_row, idx_t left_count, const Vector &right,
	                            idx_t right_count, bool row_is_false[], bool row_is_unknown[]);

private:
	DataChunk left_reference;
	Vector comparison;
};

} // namespace duckdb
