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

//! Templated radix partitioning constants, can be templated to the number of radix bits
//! See: join_hashtable.hpp
template <idx_t radix_bits>
struct RadixPartitioningConstants {
public:
	static constexpr const idx_t NUM_RADIX_BITS = radix_bits;
	static constexpr const idx_t NUM_PARTITIONS = 1 << NUM_RADIX_BITS;
	static constexpr const idx_t TMP_BUF_SIZE = 8;

public:
	static inline hash_t ApplyMask(hash_t hash) {
		return hash & Mask();
	}

private:
	static inline constexpr hash_t Mask() {
		return (hash_t(1) << NUM_RADIX_BITS) - 1;
	}
};

//! Generic radix partitioning functions
struct RadixPartitioning {
public:
	static inline idx_t NumberOfPartitions(idx_t radix_bits) {
		return 1 << radix_bits;
	}
	//! Initialize histogram for "radix_bits"
	static unique_ptr<idx_t[]> InitializeHistogram(idx_t radix_bits);
	//! Update histogram given a vector of hashes
	static void UpdateHistogram(const VectorData &hash_data, const idx_t count, const bool has_rsel, idx_t histogram[],
	                            idx_t radix_bits);
	//! Reduce a histogram from a certain number of radix bits to a lower number
	static unique_ptr<idx_t[]> ReduceHistogram(const idx_t histogram_from[], idx_t radix_bits_from,
	                                           idx_t radix_bits_to);

	//! Allocate a temporary "SWWCB" buffer for radix partitioning
	static unique_ptr<data_t[]> AllocateTempBuf(idx_t entry_size, idx_t radix_bits);

	static void Partition(data_ptr_t source_ptr, data_ptr_t tmp_buf, data_ptr_t dest_ptrs[], idx_t entry_size,
	                      idx_t count, idx_t hash_offset, idx_t radix_bits);
};

} // namespace duckdb
