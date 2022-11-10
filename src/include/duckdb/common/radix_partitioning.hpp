//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/radix_partitioning.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/types/partitioned_column_data.hpp"

namespace duckdb {

class BufferManager;
class RowLayout;
class RowDataCollection;
class Vector;
struct UnifiedVectorFormat;
struct SelectionVector;

//! Templated radix partitioning constants, can be templated to the number of radix bits
template <idx_t radix_bits>
struct RadixPartitioningConstants {
public:
	static constexpr const idx_t NUM_RADIX_BITS = radix_bits;
	static constexpr const idx_t NUM_PARTITIONS = (idx_t)1 << NUM_RADIX_BITS;
	static constexpr const idx_t TMP_BUF_SIZE = 8;

public:
	//! Apply bitmask on the highest bits, and right shift to get a number between 0 and NUM_PARTITIONS
	static inline hash_t ApplyMask(hash_t hash) {
		return (hash & MASK) >> (sizeof(hash_t) * 8 - NUM_RADIX_BITS);
	}

private:
	//! Bitmask of the highest bits
	static constexpr const hash_t MASK = hash_t(-1) ^ ((hash_t(1) << (sizeof(hash_t) * 8 - NUM_RADIX_BITS)) - 1);
};

//! Generic radix partitioning functions
struct RadixPartitioning {
public:
	static idx_t NumberOfPartitions(idx_t radix_bits) {
		return (idx_t)1 << radix_bits;
	}

	//! Select using a cutoff on the radix bits of the hash
	static idx_t Select(Vector &hashes, const SelectionVector *sel, idx_t count, idx_t radix_bits, idx_t cutoff,
	                    SelectionVector *true_sel, SelectionVector *false_sel);

	//! Partition the data in block_collection/string_heap to multiple partitions
	static void PartitionRowData(BufferManager &buffer_manager, const RowLayout &layout, const idx_t hash_offset,
	                             RowDataCollection &block_collection, RowDataCollection &string_heap,
	                             vector<unique_ptr<RowDataCollection>> &partition_block_collections,
	                             vector<unique_ptr<RowDataCollection>> &partition_string_heaps, idx_t radix_bits);
};

//! RadixPartitionedColumnData is a PartitionedColumnData that partitions input based on the radix of a hash
class RadixPartitionedColumnData : public PartitionedColumnData {
public:
	RadixPartitionedColumnData(ClientContext &context, vector<LogicalType> types, idx_t radix_bits, idx_t hash_col_idx);
	RadixPartitionedColumnData(const RadixPartitionedColumnData &other);
	~RadixPartitionedColumnData() override;

protected:
	//===--------------------------------------------------------------------===//
	// Radix Partitioning interface implementation
	//===--------------------------------------------------------------------===//
	idx_t BufferSize() const override {
		switch (radix_bits) {
		case 1:
		case 2:
		case 3:
		case 4:
			return GetBufferSize(1);
		case 5:
			return GetBufferSize(2);
		case 6:
			return GetBufferSize(3);
		default:
			return GetBufferSize(4);
		}
	}
	void InitializeAppendStateInternal(PartitionedColumnDataAppendState &state) const override;
	void ComputePartitionIndices(PartitionedColumnDataAppendState &state, DataChunk &input) override;

	static constexpr idx_t GetBufferSize(idx_t div) {
		return STANDARD_VECTOR_SIZE / div == 0 ? 1 : STANDARD_VECTOR_SIZE / div;
	}

private:
	//! The number of radix bits
	const idx_t radix_bits;
	//! The index of the column holding the hashes
	const idx_t hash_col_idx;
};

} // namespace duckdb
