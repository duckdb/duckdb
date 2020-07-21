//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/physical_window.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! PhysicalWindow implements window functions
class PhysicalWindow : public PhysicalOperator {
public:
	PhysicalWindow(vector<TypeId> types, vector<unique_ptr<Expression>> select_list,
	               PhysicalOperatorType type = PhysicalOperatorType::WINDOW);

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	//! The projection list of the SELECT statement (that contains aggregates)
	vector<unique_ptr<Expression>> select_list;

public:
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

} // namespace duckdb
