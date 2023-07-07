//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/radix_partitioning.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/types/column/partitioned_column_data.hpp"
#include "duckdb/common/types/row/partitioned_tuple_data.hpp"

namespace duckdb {

class BufferManager;
class Vector;
struct UnifiedVectorFormat;
struct SelectionVector;

//! Generic radix partitioning functions
struct RadixPartitioning {
public:
	//! 4096 partitions ought to be enough to go out-of-core properly
	static constexpr const idx_t MAX_RADIX_BITS = 12;

	//! The number of partitions for a given number of radix bits
	static inline constexpr idx_t NumberOfPartitions(idx_t radix_bits) {
		return idx_t(1) << radix_bits;
	}

	//! Inverse of NumberOfPartitions, given a number of partitions, get the number of radix bits
	static inline idx_t RadixBits(idx_t n_partitions) {
		D_ASSERT(IsPowerOfTwo(n_partitions));
		for (idx_t r = 0; r < sizeof(idx_t) * 8; r++) {
			if (n_partitions == NumberOfPartitions(r)) {
				return r;
			}
		}
		throw InternalException("RadixPartitioning::RadixBits unable to find partition count!");
	}

	//! Radix bits begin after uint16_t because these bits are used as salt in the aggregate HT
	static inline constexpr idx_t Shift(idx_t radix_bits) {
		return (sizeof(hash_t) - sizeof(uint16_t)) * 8 - radix_bits;
	}

	//! Mask of the radix bits of the hash
	static inline constexpr hash_t Mask(idx_t radix_bits) {
		return (hash_t(1 << radix_bits) - 1) << Shift(radix_bits);
	}

	//! Select using a cutoff on the radix bits of the hash
	static idx_t Select(Vector &hashes, const SelectionVector *sel, idx_t count, idx_t radix_bits, idx_t cutoff,
	                    SelectionVector *true_sel, SelectionVector *false_sel);
};

//! RadixPartitionedColumnData is a PartitionedColumnData that partitions input based on the radix of a hash
class RadixPartitionedColumnData : public PartitionedColumnData {
public:
	RadixPartitionedColumnData(ClientContext &context, vector<LogicalType> types, idx_t radix_bits, idx_t hash_col_idx);
	RadixPartitionedColumnData(const RadixPartitionedColumnData &other);
	~RadixPartitionedColumnData() override;

	idx_t GetRadixBits() const {
		return radix_bits;
	}

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
			return GetBufferSize(1 << 1);
		case 5:
			return GetBufferSize(1 << 2);
		case 6:
			return GetBufferSize(1 << 3);
		default:
			return GetBufferSize(1 << 4);
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

//! RadixPartitionedTupleData is a PartitionedTupleData that partitions input based on the radix of a hash
class RadixPartitionedTupleData : public PartitionedTupleData {
public:
	RadixPartitionedTupleData(BufferManager &buffer_manager, const TupleDataLayout &layout, idx_t radix_bits_p,
	                          idx_t hash_col_idx_p);
	RadixPartitionedTupleData(const RadixPartitionedTupleData &other);
	~RadixPartitionedTupleData() override;

	idx_t GetRadixBits() const {
		return radix_bits;
	}

private:
	void Initialize();

protected:
	//===--------------------------------------------------------------------===//
	// Radix Partitioning interface implementation
	//===--------------------------------------------------------------------===//
	void InitializeAppendStateInternal(PartitionedTupleDataAppendState &state,
	                                   TupleDataPinProperties properties) const override;
	void ComputePartitionIndices(PartitionedTupleDataAppendState &state, DataChunk &input) override;
	void ComputePartitionIndices(Vector &row_locations, idx_t count, Vector &partition_indices) const override;
	idx_t MaxPartitionIndex() const override {
		return RadixPartitioning::NumberOfPartitions(radix_bits) - 1;
	}

	bool RepartitionReverseOrder() const override {
		return true;
	}
	void RepartitionFinalizeStates(PartitionedTupleData &old_partitioned_data,
	                               PartitionedTupleData &new_partitioned_data, PartitionedTupleDataAppendState &state,
	                               idx_t finished_partition_idx) const override;

private:
	//! The number of radix bits
	const idx_t radix_bits;
	//! The index of the column holding the hashes
	const idx_t hash_col_idx;
};

} // namespace duckdb
