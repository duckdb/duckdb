//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/projection/physical_unnest.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! PhysicalWindow implements window functions
class PhysicalUnnest : public PhysicalOperator {
public:
	PhysicalUnnest(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list, idx_t estimated_cardinality,
	               PhysicalOperatorType type = PhysicalOperatorType::UNNEST);

	//! The projection list of the SELECT statement (that contains aggregates)
	vector<unique_ptr<Expression>> select_list;

public:
	// unique_ptr<OperatorState> GetOperatorState() override;
	// void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, OperatorState *state) const override;
};

} // namespace duckdb
