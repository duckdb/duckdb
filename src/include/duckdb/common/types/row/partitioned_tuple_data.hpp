//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/partitioned_tuple_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/perfect_map_set.hpp"
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

	static constexpr idx_t MAP_THRESHOLD = 32;
	perfect_map_t<list_entry_t> partition_entries;
	list_entry_t partition_entries_arr[MAP_THRESHOLD];

	vector<unique_ptr<TupleDataPinState>> partition_pin_states;
	TupleDataChunkState chunk_state;
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
	//! Get the partitioning type of this PartitionedTupleData
	PartitionedTupleDataType GetType() const;
	//! Initializes a local state for parallel partitioning that can be merged into this PartitionedTupleData
	void InitializeAppendState(PartitionedTupleDataAppendState &state,
	                           TupleDataPinProperties properties = TupleDataPinProperties::UNPIN_AFTER_DONE) const;
	//! Appends a DataChunk to this PartitionedTupleData
	void Append(PartitionedTupleDataAppendState &state, DataChunk &input);
	//! Appends rows to this PartitionedTupleData
	void Append(PartitionedTupleDataAppendState &state, TupleDataChunkState &input, idx_t count);
	//! Flushes any remaining data in the append state into this PartitionedTupleData
	void FlushAppendState(PartitionedTupleDataAppendState &state);
	//! Combine another PartitionedTupleData into this PartitionedTupleData
	void Combine(PartitionedTupleData &other);
	//! Partition a TupleDataCollection
	void Partition(TupleDataCollection &source,
	               TupleDataPinProperties properties = TupleDataPinProperties::UNPIN_AFTER_DONE);
	//! Repartition this PartitionedTupleData into the new PartitionedTupleData
	void Repartition(PartitionedTupleData &new_partitioned_data);
	//! Get the partitions in this PartitionedTupleData
	vector<unique_ptr<TupleDataCollection>> &GetPartitions();
	//! Get the count of this PartitionedTupleData
	idx_t Count() const;
	//! Get the size (in bytes) of this PartitionedTupleData
	idx_t SizeInBytes() const;

protected:
	//===--------------------------------------------------------------------===//
	// Partitioning type implementation interface
	//===--------------------------------------------------------------------===//
	//! Initialize a PartitionedTupleDataAppendState for this type of partitioning (optional)
	virtual void InitializeAppendStateInternal(PartitionedTupleDataAppendState &state,
	                                           TupleDataPinProperties properties) const {
	}
	//! Compute the partition indices for this type of partitioning for the input DataChunk and store them in the
	//! `partition_data` of the local state. If this type creates partitions on the fly (for, e.g., hive), this
	//! function is also in charge of creating new partitions and mapping the input data to a partition index
	virtual void ComputePartitionIndices(PartitionedTupleDataAppendState &state, DataChunk &input) {
		throw NotImplementedException("ComputePartitionIndices for this type of PartitionedTupleData");
	}
	//! Compute partition indices from rows (similar to function above)
	virtual void ComputePartitionIndices(Vector &row_locations, idx_t count, Vector &partition_indices) const {
		throw NotImplementedException("ComputePartitionIndices for this type of PartitionedTupleData");
	}
	//! Maximum partition index (optional)
	virtual idx_t MaxPartitionIndex() const {
		return DConstants::INVALID_INDEX;
	}

	//! Whether or not to iterate over the original partitions in reverse order when repartitioning (optional)
	virtual bool RepartitionReverseOrder() const {
		return false;
	}
	//! Finalize states while repartitioning - useful for unpinning blocks that are no longer needed (optional)
	virtual void RepartitionFinalizeStates(PartitionedTupleData &old_partitioned_data,
	                                       PartitionedTupleData &new_partitioned_data,
	                                       PartitionedTupleDataAppendState &state, idx_t finished_partition_idx) const {
	}

protected:
	//! PartitionedTupleData can only be instantiated by derived classes
	PartitionedTupleData(PartitionedTupleDataType type, BufferManager &buffer_manager, const TupleDataLayout &layout);
	PartitionedTupleData(const PartitionedTupleData &other);

	//! Create a new shared allocator
	void CreateAllocator();
	//! Builds a selection vector in the Append state for the partitions
	//! - returns true if everything belongs to the same partition - stores partition index in single_partition_idx
	void BuildPartitionSel(PartitionedTupleDataAppendState &state, idx_t count);
	//! Builds out the buffer space in the partitions
	void BuildBufferSpace(PartitionedTupleDataAppendState &state);
	//! Create a collection for a specific a partition
	unique_ptr<TupleDataCollection> CreatePartitionCollection(idx_t partition_index) const {
		return make_uniq<TupleDataCollection>(allocators->allocators[partition_index]);
	}

protected:
	PartitionedTupleDataType type;
	BufferManager &buffer_manager;
	const TupleDataLayout layout;

	mutex lock;
	shared_ptr<PartitionTupleDataAllocators> allocators;
	vector<unique_ptr<TupleDataCollection>> partitions;

public:
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
};

} // namespace duckdb
