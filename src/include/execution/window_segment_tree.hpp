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
	Value Compute(uint64_t start, uint64_t end);

private:
	void ConstructTree();
	void WindowSegmentValue(uint64_t l_idx, uint64_t begin, uint64_t end);
	void AggregateInit();
	Value AggegateFinal();

	Value aggregate;
	uint64_t n_aggregated; // for sum
	ExpressionType window_type;
	TypeId payload_type;
	unique_ptr<char[]> levels_flat_native;
	vector<uint64_t> levels_flat_start;

	ChunkCollection *input_ref;

	static constexpr uint64_t TREE_FANOUT = 64; // this should cleanly divide STANDARD_VECTOR_SIZE
};

} // namespace duckdb
