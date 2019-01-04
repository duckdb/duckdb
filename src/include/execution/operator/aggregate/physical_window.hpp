//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/aggregate/physical_window.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"
#include "common/types/tuple.hpp"
#include "execution/physical_operator.hpp"

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
	ChunkCollection window_results;
};

class WindowSegmentTree {
public:
	WindowSegmentTree(ExpressionType window_type, TypeId payload_type, ChunkCollection *input)
	    : aggregate(Value()), n_aggregated(0), window_type(window_type), payload_type(payload_type), input_ref(input) {
		ConstructTree();
	}
	Value Compute(size_t start, size_t end);

private:
	void ConstructTree();
	void WindowSegmentValue(size_t l_idx, size_t begin, size_t end);
	void AggregateInit();
	Value AggegateFinal();

	Value aggregate;
	size_t n_aggregated; // for sum
	ExpressionType window_type;
	TypeId payload_type;
	unique_ptr<char[]> levels_flat_native;
	vector<size_t> levels_flat_start;

	const size_t fanout = 64; // this should cleanly divide STANDARD_VECTOR_SIZE
	ChunkCollection *input_ref;
};

} // namespace duckdb
