//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/partitioned_column_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/pair.hpp"
#include "duckdb/common/types/column_data_allocator.hpp"
#include "duckdb/common/types/column_data_collection.hpp"

namespace duckdb {

struct PartitionedColumnDataAppendState;
class PartitionedColumnData;

//! Local state for partioning in parallel
struct PartitionedColumnDataAppendState {
public:
	explicit PartitionedColumnDataAppendState() : partition_indices(LogicalType::UBIGINT) {
	}

public:
	Vector partition_indices;
	SelectionVector partition_sel;

	vector<unique_ptr<DataChunk>> partition_buffers;
	vector<unique_ptr<ColumnDataCollection>> partitions;
	vector<ColumnDataAppendState> partition_append_states;

private:
	//! Implicit copying is not allowed
	PartitionedColumnDataAppendState(const PartitionedColumnDataAppendState &) = delete;
};

//! PartitionedColumnData represents partitioned columnar data, which serves as an interface for different flavors of
//! partitioning, e.g., radix, hive
class PartitionedColumnData {
public:
	PartitionedColumnData(ClientContext &context, vector<LogicalType> types);
	virtual ~PartitionedColumnData();

public:
	//! The types of columns in the PartitionedColumnData
	const vector<LogicalType> &Types() const {
		return types;
	}
	//! The number of columns in the PartitionedColumnData
	idx_t ColumnCount() const {
		return types.size();
	}
	//! The number of partitions in the PartitionedColumnData
	idx_t NumberOfPartitions() const {
		return partition_allocators.size();
	}
	//! The partitions in this PartitionedColumnData
	vector<unique_ptr<ColumnDataCollection>> &GetPartitions() {
		return partitions;
	}

public:
	//! Initializes a local state for parallel partitioning that can be merged into this PartitionedColumnData
	void InitializeAppendState(PartitionedColumnDataAppendState &state);

	//! Appends a DataChunk to the PartitionedColumnDataAppendState
	virtual void AppendChunk(PartitionedColumnDataAppendState &state, DataChunk &input);

	//! Combine a local state into this PartitionedColumnData
	void CombineLocalState(PartitionedColumnDataAppendState &state);

private:
	//===--------------------------------------------------------------------===//
	// Partitioning flavor implementation interface
	//===--------------------------------------------------------------------===//
	//! Initialize a PartitionedColumnDataAppendState for this flavor of partitioning (optional)
	virtual void InitializeAppendStateInternal(PartitionedColumnDataAppendState &state) {
	}

	//! Compute the partition indices for this flavor of partioning for the input DataChunk and store them in the
	//! `partition_data` of the local state. If this flavor creates partitions on the fly (for, e.g., hive), this
	//! function is also in charge of creating new partitions and mapping the input data to a partition index
	virtual void ComputePartitionIndices(PartitionedColumnDataAppendState &state, DataChunk &input) {
		throw NotImplementedException("ComputePartitionIndices for this flavor of PartitionedColumnData");
	}

protected:
	unique_ptr<ColumnDataCollection> CreateAppendPartition(idx_t partition_index) {
		return make_unique<ColumnDataCollection>(partition_allocators[partition_index], types);
	}

	unique_ptr<DataChunk> CreateAppendPartitionBuffer() {
		auto result = make_unique<DataChunk>();
		result->Initialize(Allocator::Get(context), types);
		return result;
	}

protected:
	ClientContext &context;
	vector<LogicalType> types;

	vector<shared_ptr<ColumnDataAllocator>> partition_allocators;
	vector<unique_ptr<ColumnDataCollection>> partitions;
	mutex lock;
};

} // namespace duckdb
