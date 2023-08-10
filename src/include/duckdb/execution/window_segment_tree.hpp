//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/window_segment_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

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
	WindowAggregator(AggregateObject aggr, const LogicalType &result_type_p, WindowExclusion exclude_mode_p,
	                 idx_t partition_count);
	virtual ~WindowAggregator();

	//	Build
	virtual void Sink(DataChunk &payload_chunk, SelectionVector *filter_sel, idx_t filtered);
	virtual void Finalize();

	//	Probe
	virtual unique_ptr<WindowAggregatorState> GetLocalState() const = 0;
	virtual void Evaluate(WindowAggregatorState &lstate, const DataChunk &bounds, Vector &result, idx_t count,
	                      idx_t row_idx) const = 0;

protected:
	AggregateObject aggr;
	//! The result type of the window function
	LogicalType result_type;

	//! The cardinality of the partition
	const idx_t partition_count;
	//! The size of a single aggregate state
	const idx_t state_size;
	//! Partition data chunk
	DataChunk inputs;

	//! The filtered rows in inputs.
	vector<validity_t> filter_bits;
	ValidityMask filter_mask;
	idx_t filter_pos;
	//! The state used by the aggregator to build.
	unique_ptr<WindowAggregatorState> gstate;

	//! The window exclusion clause
	WindowExclusion exclude_mode;
};

class WindowConstantAggregator : public WindowAggregator {
public:
	WindowConstantAggregator(AggregateObject aggr, const LogicalType &result_type_p, const ValidityMask &partition_mask,
	                         WindowExclusion exclude_mode_p, const idx_t count);
	~WindowConstantAggregator() override {
	}

	void Sink(DataChunk &payload_chunk, SelectionVector *filter_sel, idx_t filtered) override;
	void Finalize() override;

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
	WindowCustomAggregator(AggregateObject aggr, const LogicalType &result_type_p, WindowExclusion exclude_mode_p,
	                       idx_t partition_count);
	~WindowCustomAggregator() override;

	unique_ptr<WindowAggregatorState> GetLocalState() const override;
	void Evaluate(WindowAggregatorState &lstate, const DataChunk &bounds, Vector &result, idx_t count,
	              idx_t row_idx) const override;
};

class WindowSegmentTree : public WindowAggregator {

public:
	enum FramePart : uint8_t { FULL = 0, LEFT = 1, RIGHT = 2 };

	WindowSegmentTree(AggregateObject aggr, const LogicalType &result_type, WindowAggregationMode mode_p,
	                  WindowExclusion exclude_mode_p, idx_t count);
	~WindowSegmentTree() override;

	void Finalize() override;

	unique_ptr<WindowAggregatorState> GetLocalState() const override;
	void Evaluate(WindowAggregatorState &lstate, const DataChunk &bounds, Vector &result, idx_t count,
	              idx_t row_idx) const override;
	void EvaluateInternal(WindowAggregatorState &lstate, const idx_t *begins, const idx_t *ends, Vector &result,
	                      idx_t count, idx_t row_idx, FramePart frame_part) const;

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

} // namespace duckdb
