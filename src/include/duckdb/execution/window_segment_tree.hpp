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

namespace duckdb {

class WindowAggregateState {
public:
	WindowAggregateState(AggregateObject aggr, const LogicalType &result_type_p, idx_t partition_count);
	virtual ~WindowAggregateState();

	virtual void Sink(DataChunk &payload_chunk, SelectionVector *filter_sel, idx_t filtered);
	virtual void Finalize();
	virtual void Compute(Vector &result, idx_t rid, idx_t start, idx_t end);
	virtual void Evaluate(const idx_t *begins, const idx_t *ends, Vector &result, idx_t count);

protected:
	void AggregateInit();
	void AggegateFinal(Vector &result, idx_t rid);

	AggregateObject aggr;
	//! The result type of the window function
	LogicalType result_type;

	//! The cardinality of the partition
	const idx_t partition_count;
	//! The size of a single aggregate state
	const idx_t state_size;
	//! Data pointer that contains a single state, used for intermediate window segment aggregation
	vector<data_t> state;
	//! Reused result state container for the window functions
	Vector statef;
	//! Partition data chunk
	DataChunk inputs;
	//! The filtered rows in inputs.
	vector<validity_t> filter_bits;
	ValidityMask filter_mask;
	idx_t filter_pos;
};

class WindowConstantAggregate : public WindowAggregateState {
public:
	WindowConstantAggregate(AggregateObject aggr, const LogicalType &result_type_p, const ValidityMask &partition_mask,
	                        const idx_t count);
	~WindowConstantAggregate() override {
	}

	void Sink(DataChunk &payload_chunk, SelectionVector *filter_sel, idx_t filtered) override;
	void Finalize() override;
	void Evaluate(const idx_t *begins, const idx_t *ends, Vector &result, idx_t count) override;

private:
	//! Partition starts
	vector<idx_t> partition_offsets;
	//! Aggregate results
	unique_ptr<Vector> results;
	//! The current result partition being built/read
	idx_t partition;
	//! The current input row being built/read
	idx_t row;
	//! A vector of pointers to "state", used for intermediate window segment aggregation
	Vector statep;
	//! Shared SV for evaluation
	SelectionVector matches;
};

class WindowCustomAggregate : public WindowAggregateState {
public:
	WindowCustomAggregate(AggregateObject aggr, const LogicalType &result_type_p, idx_t partition_count);
	~WindowCustomAggregate() override;

	void Compute(Vector &result, idx_t rid, idx_t start, idx_t end) override;

private:
	//! The frame boundaries, used for the window functions
	FrameBounds frame;
};

class WindowSegmentTree : public WindowAggregateState {
public:
	using FrameBounds = std::pair<idx_t, idx_t>;

	WindowSegmentTree(AggregateObject aggr, const LogicalType &result_type, idx_t count, WindowAggregationMode mode_p);
	~WindowSegmentTree() override;

	void Finalize() override;
	void Evaluate(const idx_t *begins, const idx_t *ends, Vector &result, idx_t count) override;

private:
	void ConstructTree();
	void ExtractFrame(idx_t begin, idx_t end, data_ptr_t current_state);
	void FlushStates(bool combining);
	void WindowSegmentValue(idx_t l_idx, idx_t begin, idx_t end, data_ptr_t current_state);

	//! Use the combine API, if available
	inline bool UseCombineAPI() const {
		return mode < WindowAggregationMode::SEPARATE;
	}

	//! Input data chunk, used for leaf segment aggregation
	DataChunk leaves;
	//! The filtered rows in inputs.
	SelectionVector filter_sel;
	//! A vector of pointers to "state", used for intermediate window segment aggregation
	Vector statep;
	//! The frame boundaries, used for the window functions
	FrameBounds frame;
	//! Reused state pointers for combining segment tree levels
	Vector statel;
	//! Count of buffered values
	idx_t flush_count;

	//! The actual window segment tree: an array of aggregate states that represent all the intermediate nodes
	unsafe_unique_array<data_t> levels_flat_native;
	//! For each level, the starting location in the levels_flat_native array
	vector<idx_t> levels_flat_start;

	//! The total number of internal nodes of the tree, stored in levels_flat_native
	idx_t internal_nodes;

	//! Use the window API, if available
	WindowAggregationMode mode;

	// TREE_FANOUT needs to cleanly divide STANDARD_VECTOR_SIZE
	static constexpr idx_t TREE_FANOUT = 16;
};

} // namespace duckdb
