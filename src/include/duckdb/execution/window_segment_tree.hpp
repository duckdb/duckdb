//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/window_segment_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/common/enums/window_aggregation_mode.hpp"
#include "duckdb/execution/operator/aggregate/aggregate_object.hpp"
#include "duckdb/parser/expression/window_expression.hpp"

namespace duckdb {

class WindowAggregatorState {
public:
	WindowAggregatorState();
	virtual ~WindowAggregatorState() {
	}

	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}

	//! Allocator for aggregates
	ArenaAllocator allocator;
};

class WindowAggregator {
public:
	WindowAggregator(AggregateObject aggr, const LogicalType &result_type_p, const WindowExcludeMode exclude_mode_p,
	                 idx_t partition_count);
	virtual ~WindowAggregator();

	//	Access
	const DataChunk &GetInputs() const {
		return inputs;
	}
	const ValidityMask &GetFilterMask() const {
		return filter_mask;
	}

	//	Build
	virtual void Sink(DataChunk &payload_chunk, SelectionVector *filter_sel, idx_t filtered);
	virtual void Finalize(const FrameStats &stats);

	//	Probe
	virtual unique_ptr<WindowAggregatorState> GetLocalState() const = 0;
	virtual void Evaluate(WindowAggregatorState &lstate, const DataChunk &bounds, Vector &result, idx_t count,
	                      idx_t row_idx) const = 0;

	//! A description of the aggregator
	const AggregateObject aggr;
	//! The result type of the window function
	const LogicalType result_type;
	//! The cardinality of the partition
	const idx_t partition_count;
	//! The size of a single aggregate state
	const idx_t state_size;

protected:
	//! Partition data chunk
	DataChunk inputs;

	//! The filtered rows in inputs.
	vector<validity_t> filter_bits;
	ValidityMask filter_mask;
	idx_t filter_pos;
	//! The state used by the aggregator to build.
	unique_ptr<WindowAggregatorState> gstate;

public:
	//! The window exclusion clause
	const WindowExcludeMode exclude_mode;
};

// Used for validation
class WindowNaiveAggregator : public WindowAggregator {
public:
	WindowNaiveAggregator(AggregateObject aggr, const LogicalType &result_type_p,
	                      const WindowExcludeMode exclude_mode_p, idx_t partition_count);
	~WindowNaiveAggregator() override;

	unique_ptr<WindowAggregatorState> GetLocalState() const override;
	void Evaluate(WindowAggregatorState &lstate, const DataChunk &bounds, Vector &result, idx_t count,
	              idx_t row_idx) const override;
};

class WindowConstantAggregator : public WindowAggregator {
public:
	WindowConstantAggregator(AggregateObject aggr, const LogicalType &result_type_p, const ValidityMask &partition_mask,
	                         WindowExcludeMode exclude_mode_p, const idx_t count);
	~WindowConstantAggregator() override {
	}

	void Sink(DataChunk &payload_chunk, SelectionVector *filter_sel, idx_t filtered) override;
	void Finalize(const FrameStats &stats) override;

	unique_ptr<WindowAggregatorState> GetLocalState() const override;
	void Evaluate(WindowAggregatorState &lstate, const DataChunk &bounds, Vector &result, idx_t count,
	              idx_t row_idx) const override;

private:
	void AggregateInit();
	void AggegateFinal(Vector &result, idx_t rid);

	//! Partition starts
	vector<idx_t> partition_offsets;
	//! Aggregate results
	unique_ptr<Vector> results;
	//! The current result partition being built/read
	idx_t partition;
	//! The current input row being built/read
	idx_t row;
	//! Data pointer that contains a single state, used for intermediate window segment aggregation
	vector<data_t> state;
	//! A vector of pointers to "state", used for intermediate window segment aggregation
	Vector statep;
	//! Reused result state container for the window functions
	Vector statef;
};

class WindowCustomAggregator : public WindowAggregator {
public:
	WindowCustomAggregator(AggregateObject aggr, const LogicalType &result_type_p,
	                       const WindowExcludeMode exclude_mode_p, idx_t partition_count);
	~WindowCustomAggregator() override;

	void Finalize(const FrameStats &stats) override;

	unique_ptr<WindowAggregatorState> GetLocalState() const override;
	void Evaluate(WindowAggregatorState &lstate, const DataChunk &bounds, Vector &result, idx_t count,
	              idx_t row_idx) const override;

	//! Partition description
	unique_ptr<WindowPartitionInput> partition_input;

	//! Data pointer that contains a single state, used for global custom window state
	unique_ptr<WindowAggregatorState> gstate;
};

class WindowSegmentTree : public WindowAggregator {

public:
	WindowSegmentTree(AggregateObject aggr, const LogicalType &result_type, WindowAggregationMode mode_p,
	                  const WindowExcludeMode exclude_mode_p, idx_t count);
	~WindowSegmentTree() override;

	void Finalize(const FrameStats &stats) override;

	unique_ptr<WindowAggregatorState> GetLocalState() const override;
	void Evaluate(WindowAggregatorState &lstate, const DataChunk &bounds, Vector &result, idx_t count,
	              idx_t row_idx) const override;

public:
	void ConstructTree();

	//! Use the combine API, if available
	inline bool UseCombineAPI() const {
		return mode < WindowAggregationMode::SEPARATE;
	}

	//! The actual window segment tree: an array of aggregate states that represent all the intermediate nodes
	unsafe_unique_array<data_t> levels_flat_native;
	//! For each level, the starting location in the levels_flat_native array
	vector<idx_t> levels_flat_start;

	//! The total number of internal nodes of the tree, stored in levels_flat_native
	idx_t internal_nodes;

	//! Use the combine API, if available
	WindowAggregationMode mode;

	// TREE_FANOUT needs to cleanly divide STANDARD_VECTOR_SIZE
	static constexpr idx_t TREE_FANOUT = 16;
};

class WindowDistinctAggregator : public WindowAggregator {
public:
	using GlobalSortStatePtr = unique_ptr<GlobalSortState>;
	class DistinctSortTree;

	WindowDistinctAggregator(AggregateObject aggr, const LogicalType &result_type,
	                         const WindowExcludeMode exclude_mode_p, idx_t count, ClientContext &context);
	~WindowDistinctAggregator() override;

	//	Build
	void Sink(DataChunk &args_chunk, SelectionVector *filter_sel, idx_t filtered) override;
	void Finalize(const FrameStats &stats) override;

	//	Evaluate
	unique_ptr<WindowAggregatorState> GetLocalState() const override;
	void Evaluate(WindowAggregatorState &lstate, const DataChunk &bounds, Vector &result, idx_t count,
	              idx_t row_idx) const override;

	ClientContext &context;
	ArenaAllocator allocator;

	//	Single threaded sorting for now
	GlobalSortStatePtr global_sort;
	LocalSortState local_sort;
	idx_t payload_pos;
	idx_t memory_per_thread;

	vector<LogicalType> payload_types;
	DataChunk sort_chunk;
	DataChunk payload_chunk;

	//! The merge sort tree for the aggregate.
	unique_ptr<DistinctSortTree> merge_sort_tree;

	//! The actual window segment tree: an array of aggregate states that represent all the intermediate nodes
	unsafe_unique_array<data_t> levels_flat_native;
	//! For each level, the starting location in the levels_flat_native array
	vector<idx_t> levels_flat_start;

	//! The total number of internal nodes of the tree, stored in levels_flat_native
	idx_t internal_nodes;
};

} // namespace duckdb
