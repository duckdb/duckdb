//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/chunk_hasher.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

//! Hashes the columns of a chunk with a "hash(#0, #1, ...)" expression run through an ExpressionExecutor
class ChunkHasher {
public:
	//! Hash all columns of chunks of the given types
	ChunkHasher(ClientContext &context, const vector<LogicalType> &types);
	//! Hash the given columns of chunks of the given types
	ChunkHasher(ClientContext &context, const vector<LogicalType> &types, const vector<idx_t> &column_ids);

public:
	//! Hash all rows of the input into a flat or constant vector
	void Hash(DataChunk &input, Vector &hashes);

private:
	unique_ptr<Expression> expression;
	ExpressionExecutor executor;
};

} // namespace duckdb
