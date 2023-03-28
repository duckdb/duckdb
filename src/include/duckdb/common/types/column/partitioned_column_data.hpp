//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/column/partitioned_column_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_data_allocator.hpp"
#include "column_data_collection.hpp"
#include "duckdb/common/perfect_map_set.hpp"

namespace duckdb {

//! Local state for parallel partitioning
struct PartitionedColumnDataAppendState {
public:
	PartitionedColumnDataAppendState() : partition_indices(LogicalType::UBIGINT) {
	}

public:
	Vector partition_indices;
	SelectionVector partition_sel;
	perfect_map_t<list_entry_t> partition_entries;
	DataChunk slice_chunk;

	vector<unique_ptr<ColumnDataAppendState>> partition_append_states;
};

enum class PartitionedColumnDataType : uint8_t {
	INVALID,
	//! Radix partitioning on a hash column
	RADIX,
	//! Hive-style multi-field partitioning
	HIVE
};

//! Shared allocators for parallel partitioning
struct PartitionColumnDataAllocators {
	mutex lock;
	vector<shared_ptr<ColumnDataAllocator>> allocators;
};

//! PartitionedColumnData represents partitioned columnar data, which serves as an interface for different types of
//! partitioning, e.g., radix, hive
class PartitionedColumnData {
public:
	unique_ptr<PartitionedColumnData> CreateShared();
	virtual ~PartitionedColumnData();

public:
	//! Initializes a local state for parallel partitioning that can be merged into this PartitionedColumnData
	void InitializeAppendState(PartitionedColumnDataAppendState &state) const;
	//! Appends a DataChunk to this PartitionedColumnData
	void Append(PartitionedColumnDataAppendState &state, DataChunk &input);
	//! Combine another PartitionedColumnData into this PartitionedColumnData
	void Combine(PartitionedColumnData &other);
	//! Get the partitions in this PartitionedColumnData
	vector<unique_ptr<ColumnDataCollection>> &GetPartitions();

protected:
	//===--------------------------------------------------------------------===//
	// Partitioning type implementation interface
	//===--------------------------------------------------------------------===//
	//! Initialize a PartitionedColumnDataAppendState for this type of partitioning (optional)
	virtual void InitializeAppendStateInternal(PartitionedColumnDataAppendState &state) const {
	}
	//! Compute the partition indices for this type of partitioning for the input DataChunk and store them in the
	//! `partition_data` of the local state. If this type creates partitions on the fly (for, e.g., hive), this
	//! function is also in charge of creating new partitions and mapping the input data to a partition index
	virtual void ComputePartitionIndices(PartitionedColumnDataAppendState &state, DataChunk &input) {
		throw NotImplementedException("ComputePartitionIndices for this type of PartitionedColumnData");
	}

protected:
	//! PartitionedColumnData can only be instantiated by derived classes
	PartitionedColumnData(PartitionedColumnDataType type, ClientContext &context, vector<LogicalType> types);
	PartitionedColumnData(const PartitionedColumnData &other);

	//! Create a new shared allocator
	void CreateAllocator();
	//! Create a collection for a specific a partition
	unique_ptr<ColumnDataCollection> CreatePartitionCollection(idx_t partition_index) const {
		return make_unique<ColumnDataCollection>(allocators->allocators[partition_index], types);
	}

protected:
	PartitionedColumnDataType type;
	ClientContext &context;
	vector<LogicalType> types;

	mutex lock;
	shared_ptr<PartitionColumnDataAllocators> allocators;
	vector<unique_ptr<ColumnDataCollection>> partitions;
};

} // namespace duckdb
