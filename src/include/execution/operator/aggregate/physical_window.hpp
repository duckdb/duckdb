//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/aggregate/physical_window.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"
#include "common/types/chunk_collection.hpp"

namespace duckdb {

//! PhysicalWindow implements window functions
class PhysicalWindow : public PhysicalOperator {
public:
	PhysicalWindow(LogicalOperator &op, vector<unique_ptr<Expression>> select_list,
	                  PhysicalOperatorType type = PhysicalOperatorType::WINDOW);

	void _GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	unique_ptr<PhysicalOperatorState> GetOperatorState(ExpressionExecutor *parent) override;

	//! The projection list of the SELECT statement (that contains aggregates)
	vector<unique_ptr<Expression>> select_list;
};

//! The operator state of the window
class PhysicalWindowOperatorState : public PhysicalOperatorState {
public:
	PhysicalWindowOperatorState(PhysicalOperator *child, ExpressionExecutor *parent_executor)
		    : PhysicalOperatorState(child, parent_executor), position(0) {
		}

	size_t position;
	ChunkCollection tuples;
	unique_ptr<uint64_t[]> sorted_vector;
};

} // namespace duckdb
