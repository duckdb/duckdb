//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/radix_partitioning.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/fast_mem.hpp"

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
	static constexpr const idx_t NUM_PARTITIONS = 1 << NUM_RADIX_BITS;
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
		return 1 << radix_bits;
	}

	//! Initialize histogram for "radix_bits"
	static unique_ptr<idx_t[]> InitializeHistogram(idx_t radix_bits);
	//! Update histogram given a vector of hashes
	static void UpdateHistogram(const UnifiedVectorFormat &hash_data, const idx_t count, const bool has_rsel,
	                            idx_t histogram[], idx_t radix_bits);
	//! Reduce a histogram from a certain number of radix bits to a lower number
	static unique_ptr<idx_t[]> ReduceHistogram(const idx_t histogram_from[], idx_t radix_bits_from,
	                                           idx_t radix_bits_to);
	//! Partition the data in block_collection/string_heap to multiple partitions
	static void Partition(BufferManager &buffer_manager, const RowLayout &layout, const idx_t hash_offset,
	                      RowDataCollection &block_collection, RowDataCollection &string_heap,
	                      vector<unique_ptr<RowDataCollection>> &partition_block_collections,
	                      vector<unique_ptr<RowDataCollection>> &partition_string_heaps, idx_t radix_bits);
	//! Select using a cutoff on the radix bits of the hash
	static idx_t Select(Vector &hashes, const SelectionVector *sel, idx_t count, idx_t radix_bits, idx_t cutoff,
	                    SelectionVector *true_sel, SelectionVector *false_sel);
};

} // namespace duckdb
