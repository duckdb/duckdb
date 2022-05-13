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
	static constexpr const idx_t NUM_RADIX_BITS = radix_bits;
	static constexpr const idx_t PARTITIONS = 1 << NUM_RADIX_BITS;
	static constexpr const idx_t TMP_BUF_SIZE = 8;

	template <idx_t radix_pass>
	static inline constexpr hash_t Mask() {
		return (hash_t(1) << (NUM_RADIX_BITS * (radix_pass + 1))) - 1 - Mask<radix_pass - 1>();
	}

	template <>
	constexpr inline hash_t Mask<0>() {
		return (hash_t(1) << NUM_RADIX_BITS) - 1;
	}
};

//! Generic radix partitioning functions
struct RadixPartitioning {
public:
	template <class CONSTANTS>
	static void UpdateHistogram(const VectorData &hash_data, const idx_t count, const bool has_rsel, idx_t histogram[],
	                            idx_t radix_pass) {
		D_ASSERT(radix_pass * CONSTANTS::NUM_RADIX_BITS <= sizeof(hash_t) * 8);
		switch (radix_pass) {
		case 0:
			return RadixPartitioning::UpdateHistogramInternal<CONSTANTS, 0>(hash_data, count, has_rsel, histogram);
		case 1:
			return RadixPartitioning::UpdateHistogramInternal<CONSTANTS, 1>(hash_data, count, has_rsel, histogram);
		case 2:
			return RadixPartitioning::UpdateHistogramInternal<CONSTANTS, 2>(hash_data, count, has_rsel, histogram);
		case 3:
			return RadixPartitioning::UpdateHistogramInternal<CONSTANTS, 3>(hash_data, count, has_rsel, histogram);
		case 4:
			return RadixPartitioning::UpdateHistogramInternal<CONSTANTS, 4>(hash_data, count, has_rsel, histogram);
		default:
			throw InternalException("Too many radix passes in UpdateHistogram!");
		}
	}

	template <class CONSTANTS>
	static unique_ptr<data_t[]> AllocateTempBuf(idx_t entry_size) {
		return unique_ptr<data_t[]>(new data_t[CONSTANTS::TMP_BUF_SIZE * CONSTANTS::PARTITIONS * entry_size]);
	}

	template <class CONSTANTS>
	static void Partition(data_ptr_t source_ptr, data_ptr_t tmp_buf, data_ptr_t dest_ptrs[], idx_t entry_size,
	                      idx_t count, idx_t hash_offset, idx_t radix_pass) {
		D_ASSERT(radix_pass * CONSTANTS::NUM_RADIX_BITS <= sizeof(hash_t) * 8);
		switch (radix_pass) {
		case 0:
			return RadixPartitioning::PartitionInternal<CONSTANTS, 0>(source_ptr, tmp_buf, dest_ptrs, entry_size, count,
			                                                          hash_offset);
		case 1:
			return RadixPartitioning::PartitionInternal<CONSTANTS, 1>(source_ptr, tmp_buf, dest_ptrs, entry_size, count,
			                                                          hash_offset);
		case 2:
			return RadixPartitioning::PartitionInternal<CONSTANTS, 2>(source_ptr, tmp_buf, dest_ptrs, entry_size, count,
			                                                          hash_offset);
		case 3:
			return RadixPartitioning::PartitionInternal<CONSTANTS, 3>(source_ptr, tmp_buf, dest_ptrs, entry_size, count,
			                                                          hash_offset);
		case 4:
			return RadixPartitioning::PartitionInternal<CONSTANTS, 4>(source_ptr, tmp_buf, dest_ptrs, entry_size, count,
			                                                          hash_offset);
		default:
			throw InternalException("Too many radix passes in Partition!");
		}
	}

private:
	template <class CONSTANTS, idx_t radix_pass>
	static inline hash_t ApplyMask(hash_t hash) {
		return (hash & CONSTANTS::template Mask<radix_pass>()) >> (CONSTANTS::NUM_RADIX_BITS * radix_pass);
	}

	template <class CONSTANTS, idx_t pass>
	static void UpdateHistogramInternal(const VectorData &hash_data, const idx_t count, const bool has_rsel,
	                                    idx_t histogram[]) {
		const auto hashes = (hash_t *)hash_data.data;
		if (has_rsel) {
			for (idx_t i = 0; i < count; i++) {
				auto idx = hash_data.sel->get_index(i);
				histogram[ApplyMask<CONSTANTS, pass>(hashes[idx])]++;
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				histogram[ApplyMask<CONSTANTS, pass>(hashes[i])]++;
			}
		}
	}

	template <class CONSTANTS, idx_t radix_pass>
	static void PartitionInternal(data_ptr_t source_ptr, const data_ptr_t tmp_buf, data_ptr_t dest_ptrs[],
	                              const idx_t entry_size, const idx_t count, const idx_t hash_offset) {
		// Initialize temporal buffer count
		idx_t pos[CONSTANTS::PARTITIONS];
		for (idx_t idx = 0; idx < CONSTANTS::PARTITIONS; idx++) {
			pos[idx] = idx * CONSTANTS::TMP_BUF_SIZE;
		}
		// Partition
		for (idx_t i = 0; i < count; i++) {
			auto idx = ApplyMask<CONSTANTS, radix_pass>(Load<hash_t>(source_ptr + hash_offset));
			// Temporal write
			FastMemcpy(tmp_buf + pos[idx] * entry_size, source_ptr, entry_size);
			source_ptr += entry_size;
			if (++pos[idx] & (CONSTANTS::TMP_BUF_SIZE - 1) == 0) {
				// Non-temporal write
				pos[idx] -= CONSTANTS::TMP_BUF_SIZE;
				memcpy(dest_ptrs[idx], tmp_buf + pos[idx] * entry_size, CONSTANTS::TMP_BUF_SIZE * entry_size);
				dest_ptrs[idx] += CONSTANTS::TMP_BUF_SIZE * entry_size;
			}
		}
		// Cleanup
		for (idx_t idx = 0; idx < CONSTANTS::PARTITIONS; idx++) {
			auto rest = pos[idx] & (CONSTANTS::TMP_BUF_SIZE - 1);
			pos[idx] -= rest;
			memcpy(dest_ptrs[idx], tmp_buf + pos[idx] * entry_size, rest * entry_size);
		}
	}
};

} // namespace duckdb
