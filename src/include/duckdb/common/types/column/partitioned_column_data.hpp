//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/column/partitioned_column_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/fixed_size_map.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/perfect_map_set.hpp"
#include "duckdb/common/types/column/column_data_allocator.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

//! Local state for parallel partitioning
struct PartitionedColumnDataAppendState {
public:
	PartitionedColumnDataAppendState() : partition_indices(LogicalType::UBIGINT) {
	}

public:
	Vector partition_indices;
	SelectionVector partition_sel;

	static constexpr idx_t MAP_THRESHOLD = 256;
	perfect_map_t<list_entry_t> partition_entries;
	fixed_size_map_t<list_entry_t> fixed_partition_entries;

	DataChunk slice_chunk;

	vector<unique_ptr<DataChunk>> partition_buffers;
	vector<unique_ptr<ColumnDataAppendState>> partition_append_states;

public:
	template <bool fixed>
	typename std::conditional<fixed, fixed_size_map_t<list_entry_t>, perfect_map_t<list_entry_t>>::type &GetMap() {
		throw NotImplementedException("PartitionedColumnDataAppendState::GetMap for boolean value");
	}

	optional_idx GetPartitionIndexIfSinglePartition(const bool use_fixed_size_map) {
		optional_idx result;
		if (use_fixed_size_map) {
			if (fixed_partition_entries.size() == 1) {
				result = fixed_partition_entries.begin().GetKey();
			}
		} else {
			if (partition_entries.size() == 1) {
				result = partition_entries.begin()->first;
			}
		}
		return result;
	}
};

template <>
inline perfect_map_t<list_entry_t> &PartitionedColumnDataAppendState::GetMap<false>() {
	return partition_entries;
}

template <>
inline fixed_size_map_t<list_entry_t> &PartitionedColumnDataAppendState::GetMap<true>() {
	return fixed_partition_entries;
}

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
	//! Flushes any remaining data in the append state into this PartitionedColumnData
	void FlushAppendState(PartitionedColumnDataAppendState &state);
	//! Combine another PartitionedColumnData into this PartitionedColumnData
	void Combine(PartitionedColumnData &other);
	//! Get the partitions in this PartitionedColumnData
	vector<unique_ptr<ColumnDataCollection>> &GetPartitions();

protected:
	//===--------------------------------------------------------------------===//
	// Partitioning type implementation interface
	//===--------------------------------------------------------------------===//
	//! Size of the buffers in the append states for this type of partitioning (default 128)
	virtual idx_t BufferSize() const {
		return MinValue<idx_t>(128, STANDARD_VECTOR_SIZE);
	}
	//! Initialize a PartitionedColumnDataAppendState for this type of partitioning (optional)
	virtual void InitializeAppendStateInternal(PartitionedColumnDataAppendState &state) const {
	}
	//! Compute the partition indices for this type of partitioning for the input DataChunk and store them in the
	//! `partition_data` of the local state. If this type creates partitions on the fly (for, e.g., hive), this
	//! function is also in charge of creating new partitions and mapping the input data to a partition index
	virtual void ComputePartitionIndices(PartitionedColumnDataAppendState &state, DataChunk &input) {
		throw NotImplementedException("ComputePartitionIndices for this type of PartitionedColumnData");
	}
	//!

	//! Maximum partition index (optional)
	virtual idx_t MaxPartitionIndex() const {
		return DConstants::INVALID_INDEX;
	}

protected:
	//! PartitionedColumnData can only be instantiated by derived classes
	PartitionedColumnData(PartitionedColumnDataType type, ClientContext &context, vector<LogicalType> types);
	PartitionedColumnData(const PartitionedColumnData &other);

	//! If the buffer is half full, we append to the partition
	inline idx_t HalfBufferSize() const {
		D_ASSERT(IsPowerOfTwo(BufferSize()));
		return BufferSize() / 2;
	}
	//! Create a new shared allocator
	void CreateAllocator();
	//! Whether to use fixed size map or regular map
	bool UseFixedSizeMap() const;
	//! Builds a selection vector in the Append state for the partitions
	//! - returns true if everything belongs to the same partition - stores partition index in single_partition_idx
	void BuildPartitionSel(PartitionedColumnDataAppendState &state, const idx_t append_count) const;
	template <bool fixed>
	static void BuildPartitionSel(PartitionedColumnDataAppendState &state, const idx_t append_count);
	//! Appends a DataChunk to this PartitionedColumnData
	template <bool fixed>
	void AppendInternal(PartitionedColumnDataAppendState &state, DataChunk &input);
	//! Create a collection for a specific a partition
	unique_ptr<ColumnDataCollection> CreatePartitionCollection(idx_t partition_index) const {
		return make_uniq<ColumnDataCollection>(allocators->allocators[partition_index], types);
	}
	//! Create a DataChunk used for buffering appends to the partition
	unique_ptr<DataChunk> CreatePartitionBuffer() const;

protected:
	PartitionedColumnDataType type;
	ClientContext &context;
	vector<LogicalType> types;

	mutex lock;
	shared_ptr<PartitionColumnDataAllocators> allocators;
	vector<unique_ptr<ColumnDataCollection>> partitions;

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

} // namespace duckdb
