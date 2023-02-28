//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/partitioned_tuple_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/row/tuple_data_allocator.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"

namespace duckdb {

//! Local state for parallel partitioning
struct PartitionedTupleDataAppendState {
public:
	PartitionedTupleDataAppendState() : partition_indices(LogicalType::UBIGINT) {
	}

public:
	Vector partition_indices;
	SelectionVector partition_sel;
	DataChunk slice_chunk;

	vector<unique_ptr<DataChunk>> partition_buffers;
	vector<unique_ptr<TupleDataAppendState>> partition_append_states;
};

enum class PartitionedTupleDataType : uint8_t {
	INVALID,
	//! Radix partitioning on a hash column
	RADIX
};

//! Shared allocators for parallel partitioning
struct PartitionTupleDataAllocators {
	mutex lock;
	vector<shared_ptr<TupleDataAllocator>> allocators;
};

//! PartitionedTupleData represents partitioned row data, which serves as an interface for different types of
//! partitioning, e.g., radix, hive
class PartitionedTupleData {
public:
	unique_ptr<PartitionedTupleData> CreateShared();
	virtual ~PartitionedTupleData();

public:
	//! Initializes a local state for parallel partitioning that can be merged into this PartitionedTupleData
	void InitializeAppendState(PartitionedTupleDataAppendState &state) const;
	//! Appends a DataChunk to this PartitionedTupleData
	void Append(PartitionedTupleDataAppendState &state, DataChunk &input);
	//! Flushes any remaining data in the append state into this PartitionedTupleData
	void FlushAppendState(PartitionedTupleDataAppendState &state);
	//! Combine another PartitionedTupleData into this PartitionedTupleData
	void Combine(PartitionedTupleData &other);
	//! Get the partitions in this PartitionedTupleData
	vector<unique_ptr<TupleDataCollection>> &GetPartitions();

protected:
	//===--------------------------------------------------------------------===//
	// Partitioning type implementation interface
	//===--------------------------------------------------------------------===//
	//! Size of the buffers in the append states for this type of partitioning (default 128)
	virtual idx_t BufferSize() const {
		return MinValue<idx_t>(128, STANDARD_VECTOR_SIZE);
	}
	//! Initialize a PartitionedTupleDataAppendState for this type of partitioning (optional)
	virtual void InitializeAppendStateInternal(PartitionedTupleDataAppendState &state) const {
	}
	//! Compute the partition indices for this type of partitioning for the input DataChunk and store them in the
	//! `partition_data` of the local state. If this type creates partitions on the fly (for, e.g., hive), this
	//! function is also in charge of creating new partitions and mapping the input data to a partition index
	virtual void ComputePartitionIndices(PartitionedTupleDataAppendState &state, DataChunk &input) {
		throw NotImplementedException("ComputePartitionIndices for this type of PartitionedTupleData");
	}

protected:
	//! PartitionedTupleData can only be instantiated by derived classes
	PartitionedTupleData(PartitionedTupleDataType type, ClientContext &context, TupleDataLayout layout);
	PartitionedTupleData(const PartitionedTupleData &other);

	//! If the buffer is half full, we append to the partition
	inline idx_t HalfBufferSize() const {
		D_ASSERT((BufferSize() & (BufferSize() - 1)) == 0); // BufferSize should be a power of two
		return BufferSize() / 2;
	}
	//! Create a new shared allocator
	void CreateAllocator();
	//! Create a collection for a specific a partition
	unique_ptr<TupleDataCollection> CreatePartitionCollection(idx_t partition_index) const {
		return make_unique<TupleDataCollection>(allocators->allocators[partition_index]);
	}
	//! Create a DataChunk used for buffering appends to the partition
	unique_ptr<DataChunk> CreatePartitionBuffer() const;

protected:
	PartitionedTupleDataType type;
	ClientContext &context;
	TupleDataLayout layout;

	mutex lock;
	shared_ptr<PartitionTupleDataAllocators> allocators;
	vector<unique_ptr<TupleDataCollection>> partitions;
};

} // namespace duckdb
