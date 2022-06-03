//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/window_segment_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/common/enums/window_aggregation_mode.hpp"

namespace duckdb {

class WindowSegmentTree {
public:
	using FrameBounds = std::pair<idx_t, idx_t>;

	WindowSegmentTree(AggregateFunction &aggregate, FunctionData *bind_info, const LogicalType &result_type,
	                  ChunkCollection *input, const ValidityMask &filter_mask, WindowAggregationMode mode);
	~WindowSegmentTree();

	//! First row contains the result.
	void Compute(Vector &result, idx_t rid, idx_t start, idx_t end);

private:
	void ConstructTree();
	void ExtractFrame(idx_t begin, idx_t end);
	void WindowSegmentValue(idx_t l_idx, idx_t begin, idx_t end);
	void AggregateInit();
	void AggegateFinal(Vector &result, idx_t rid);

	//! Use the window API, if available
	inline bool UseWindowAPI() const {
		return mode < WindowAggregationMode::COMBINE;
	}
	//! Use the combine API, if available
	inline bool UseCombineAPI() const {
		return mode < WindowAggregationMode::SEPARATE;
	}

	//! The aggregate that the window function is computed over
	AggregateFunction aggregate;
	//! The bind info of the aggregate
	FunctionData *bind_info;
	//! The result type of the window function
	LogicalType result_type;

	//! Data pointer that contains a single state, used for intermediate window segment aggregation
	vector<data_t> state;
	//! Input data chunk, used for intermediate window segment aggregation
	DataChunk inputs;
	//! The filtered rows in inputs.
	SelectionVector filter_sel;
	//! A vector of pointers to "state", used for intermediate window segment aggregation
	Vector statep;
	//! The frame boundaries, used for the window functions
	FrameBounds frame;
	//! The active data in the inputs. Used for the window functions
	FrameBounds active;
	//! Reused result state container for the window functions
	Vector statev;

	//! The actual window segment tree: an array of aggregate states that represent all the intermediate nodes
	unique_ptr<data_t[]> levels_flat_native;
	//! For each level, the starting location in the levels_flat_native array
	vector<idx_t> levels_flat_start;

	//! The total number of internal nodes of the tree, stored in levels_flat_native
	idx_t internal_nodes;

	//! The (sorted) input chunk collection on which the tree is built
	ChunkCollection *input_ref;

	//! The filtered rows in input_ref.
	const ValidityMask &filter_mask;

	//! Use the window API, if available
	WindowAggregationMode mode;

	// TREE_FANOUT needs to cleanly divide STANDARD_VECTOR_SIZE
	static constexpr idx_t TREE_FANOUT = 64;
};

} // namespace duckdb
