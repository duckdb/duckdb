//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/window_segment_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"
#include "common/types/tuple.hpp"
#include "execution/physical_operator.hpp"

namespace duckdb {

class WindowSegmentTree {
public:
	WindowSegmentTree(ExpressionType window_type, TypeId payload_type, ChunkCollection *input)
	    : aggregate(Value()), n_aggregated(0), window_type(window_type), payload_type(payload_type), input_ref(input) {
		ConstructTree();
	}
	Value Compute(index_t start, index_t end);

private:
	void ConstructTree();
	void WindowSegmentValue(index_t l_idx, index_t begin, index_t end);
	void AggregateInit();
	Value AggegateFinal();

	Value aggregate;
	index_t n_aggregated; // for sum
	ExpressionType window_type;
	TypeId payload_type;
	unique_ptr<data_t[]> levels_flat_native;
	vector<index_t> levels_flat_start;

	ChunkCollection *input_ref;

	static constexpr index_t TREE_FANOUT = 64; // this should cleanly divide STANDARD_VECTOR_SIZE
};

} // namespace duckdb
