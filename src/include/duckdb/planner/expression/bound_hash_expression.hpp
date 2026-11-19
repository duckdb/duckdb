//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_hash_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"

namespace duckdb {
class ClientContext;
class DataChunk;
class ExpressionExecutor;
class Vector;

//! Helper methods to hash the columns of a chunk through an ExpressionExecutor
struct BoundHashExpression {
	//! Create "hash(#column_ids[0], #column_ids[1], ...)" over input columns of the given types, without collations
	static unique_ptr<Expression> CreateOnColumns(ClientContext &context, const vector<LogicalType> &types,
	                                              const vector<idx_t> &column_ids);
	//! Create "hash(#0, #1, ...)" over all input columns of the given types
	static unique_ptr<Expression> CreateOnColumns(ClientContext &context, const vector<LogicalType> &types);
	//! Execute the hash expression expr_idx of the executor over the input into a flat or constant vector
	static void Execute(ExpressionExecutor &executor, DataChunk &input, Vector &hashes, idx_t expr_idx = 0);
};

} // namespace duckdb
