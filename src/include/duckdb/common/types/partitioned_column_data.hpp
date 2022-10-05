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

enum class PartitionedColumnDataType : uint8_t { RADIX, INVALID };

struct PartitionedColumnDataAppendState;
class PartitionedColumnData;

//! Local state for parallel partitioning
struct PartitionedColumnDataAppendState {
public:
	explicit PartitionedColumnDataAppendState() : partition_indices(LogicalType::UBIGINT) {
	}

public:
	Vector partition_indices;
	SelectionVector partition_sel;
	DataChunk slice_chunk;

	vector<unique_ptr<DataChunk>> partition_buffers;
	vector<ColumnDataAppendState> partition_append_states;

private:
	//! Implicit copying is not allowed
	PartitionedColumnDataAppendState(const PartitionedColumnDataAppendState &) = delete;
};

//! Shared allocators for parallel partitioning
struct PartitionAllocators {
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
	//! The number of partitions in the PartitionedColumnData
	idx_t NumberOfPartitions() const {
		return allocators->allocators.size();
	}
	//! The partitions in this PartitionedColumnData
	vector<unique_ptr<ColumnDataCollection>> &GetPartitions() {
		return partitions;
	}

public:
	//! Initializes a local state for parallel partitioning that can be merged into this PartitionedColumnData
	void InitializeAppendState(PartitionedColumnDataAppendState &state);
	//! Appends a DataChunk to this PartitionedColumnData
	void Append(PartitionedColumnDataAppendState &state, DataChunk &input);
	//! Flushes any remaining data in the append state into this PartitionedColumnData
	void FlushAppendState(PartitionedColumnDataAppendState &state);
	//! Combine another PartitionedColumnData into this PartitionedColumnData
	void Combine(PartitionedColumnData &other);

private:
	//===--------------------------------------------------------------------===//
	// Partitioning type implementation interface
	//===--------------------------------------------------------------------===//
	//! Size of the buffers in the append states for this type of partitioning
	virtual idx_t BufferSize() const {
		return 128;
	}
	//! Initialize a PartitionedColumnDataAppendState for this type of partitioning (optional)
	virtual void InitializeAppendStateInternal(PartitionedColumnDataAppendState &state) const {
	}
	//! Compute the partition indices for this type of partioning for the input DataChunk and store them in the
	//! `partition_data` of the local state. If this type creates partitions on the fly (for, e.g., hive), this
	//! function is also in charge of creating new partitions and mapping the input data to a partition index
	virtual void ComputePartitionIndices(PartitionedColumnDataAppendState &state, DataChunk &input) {
		throw NotImplementedException("ComputePartitionIndices for this type of PartitionedColumnData");
	}

protected:
	//! PartitionedColumnData can only be instantiated by derived classes
	PartitionedColumnData(PartitionedColumnDataType type, ClientContext &context, vector<LogicalType> types);
	PartitionedColumnData(const PartitionedColumnData &other);

	inline idx_t HalfBufferSize() const {
		// Buffersize should be a power of two
		D_ASSERT((BufferSize() & (BufferSize() - 1)) == 0);
		return BufferSize() / 2;
	}
	//! Create a new shared allocator
	void CreateAllocator();
	//! Create a collection for a specific a partition
	unique_ptr<ColumnDataCollection> CreateCollectionForPartition(idx_t partition_index) const {
		return make_unique<ColumnDataCollection>(allocators->allocators[partition_index], types);
	}
	//! Create a DataChunk used for buffering appends to the partition
	unique_ptr<DataChunk> CreateAppendPartitionBuffer() const {
		auto result = make_unique<DataChunk>();
		result->Initialize(Allocator::Get(context), types, BufferSize());
		return result;
	}

protected:
	PartitionedColumnDataType type;
	ClientContext &context;
	vector<LogicalType> types;

	mutex lock;
	shared_ptr<PartitionAllocators> allocators;
	vector<unique_ptr<ColumnDataCollection>> partitions;
};

} // namespace duckdb
