#include "duckdb/common/radix_partitioning.hpp"

#include <functional>

namespace duckdb {

template <class OP, class RETURN_TYPE, typename... ARGS>
RETURN_TYPE RadixBitsSwitch(idx_t radix_bits, ARGS &&...args) {
	D_ASSERT(radix_bits <= sizeof(hash_t) * 8);
	switch (radix_bits) {
	case 0:
		return OP::template Operation<0>(std::forward<ARGS>(args)...);
	case 1:
		return OP::template Operation<1>(std::forward<ARGS>(args)...);
	case 2:
		return OP::template Operation<2>(std::forward<ARGS>(args)...);
	case 3:
		return OP::template Operation<3>(std::forward<ARGS>(args)...);
	case 4:
		return OP::template Operation<4>(std::forward<ARGS>(args)...);
	case 5:
		return OP::template Operation<5>(std::forward<ARGS>(args)...);
	case 6:
		return OP::template Operation<6>(std::forward<ARGS>(args)...);
	case 7:
		return OP::template Operation<7>(std::forward<ARGS>(args)...);
	case 8:
		return OP::template Operation<8>(std::forward<ARGS>(args)...);
	case 9:
		return OP::template Operation<9>(std::forward<ARGS>(args)...);
	case 10:
		return OP::template Operation<10>(std::forward<ARGS>(args)...);
	default:
		throw InternalException("TODO");
	}
}

template <class OP, class RETURN_TYPE, idx_t radix_bits_1, typename... ARGS>
RETURN_TYPE DoubleRadixBitsSwitch2(idx_t radix_bits_2, ARGS &&...args) {
	D_ASSERT(radix_bits_2 <= sizeof(hash_t) * 8);
	switch (radix_bits_2) {
	case 0:
		return OP::template Operation<radix_bits_1, 0>(std::forward<ARGS>(args)...);
	case 1:
		return OP::template Operation<radix_bits_1, 1>(std::forward<ARGS>(args)...);
	case 2:
		return OP::template Operation<radix_bits_1, 2>(std::forward<ARGS>(args)...);
	case 3:
		return OP::template Operation<radix_bits_1, 3>(std::forward<ARGS>(args)...);
	case 4:
		return OP::template Operation<radix_bits_1, 4>(std::forward<ARGS>(args)...);
	case 5:
		return OP::template Operation<radix_bits_1, 5>(std::forward<ARGS>(args)...);
	case 6:
		return OP::template Operation<radix_bits_1, 6>(std::forward<ARGS>(args)...);
	case 7:
		return OP::template Operation<radix_bits_1, 7>(std::forward<ARGS>(args)...);
	case 8:
		return OP::template Operation<radix_bits_1, 8>(std::forward<ARGS>(args)...);
	case 9:
		return OP::template Operation<radix_bits_1, 9>(std::forward<ARGS>(args)...);
	case 10:
		return OP::template Operation<radix_bits_1, 10>(std::forward<ARGS>(args)...);
	default:
		throw InternalException("TODO");
	}
}

template <class OP, class RETURN_TYPE, typename... ARGS>
RETURN_TYPE DoubleRadixBitsSwitch1(idx_t radix_bits_1, idx_t radix_bits_2, ARGS &&...args) {
	D_ASSERT(radix_bits_1 <= sizeof(hash_t) * 8);
	switch (radix_bits_1) {
	case 0:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 0>(radix_bits_2, std::forward<ARGS>(args)...);
	case 1:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 1>(radix_bits_2, std::forward<ARGS>(args)...);
	case 2:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 2>(radix_bits_2, std::forward<ARGS>(args)...);
	case 3:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 3>(radix_bits_2, std::forward<ARGS>(args)...);
	case 4:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 4>(radix_bits_2, std::forward<ARGS>(args)...);
	case 5:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 5>(radix_bits_2, std::forward<ARGS>(args)...);
	case 6:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 6>(radix_bits_2, std::forward<ARGS>(args)...);
	case 7:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 7>(radix_bits_2, std::forward<ARGS>(args)...);
	case 8:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 8>(radix_bits_2, std::forward<ARGS>(args)...);
	case 9:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 9>(radix_bits_2, std::forward<ARGS>(args)...);
	case 10:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 10>(radix_bits_2, std::forward<ARGS>(args)...);
	default:
		throw InternalException("TODO");
	}
}

unique_ptr<idx_t[]> RadixPartitioning::InitializeHistogram(idx_t radix_bits) {
	auto result = unique_ptr<idx_t[]>(new idx_t[1 << radix_bits]);
	memset(result.get(), 0, (1 << radix_bits) * sizeof(idx_t));
	return result;
}

struct UpdateHistogramFunctor {
	template <idx_t radix_bits>
	static void Operation(const VectorData &hash_data, const idx_t count, const bool has_rsel, idx_t histogram[]) {
		using CONSTANTS = RadixPartitioningConstants<radix_bits>;

		const auto hashes = (hash_t *)hash_data.data;
		if (has_rsel) {
			for (idx_t i = 0; i < count; i++) {
				auto idx = hash_data.sel->get_index(i);
				histogram[CONSTANTS::ApplyMask(hashes[idx])]++;
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				histogram[CONSTANTS::ApplyMask(hashes[i])]++;
			}
		}
	}
};

void RadixPartitioning::UpdateHistogram(const VectorData &hash_data, const idx_t count, const bool has_rsel,
                                        idx_t histogram[], idx_t radix_bits) {
	return RadixBitsSwitch<UpdateHistogramFunctor, void>(radix_bits, hash_data, count, has_rsel, histogram);
}

struct ReduceHistogramFunctor {
	template <idx_t radix_bits_from, idx_t radix_bits_to>
	static unique_ptr<idx_t[]> Operation(const idx_t histogram_from[]) {
		using CONSTANTS_FROM = RadixPartitioningConstants<radix_bits_from>;
		using CONSTANTS_TO = RadixPartitioningConstants<radix_bits_to>;

		auto result = RadixPartitioning::InitializeHistogram(radix_bits_to);
		auto histogram_to = result.get();
		for (idx_t i = 0; i < CONSTANTS_FROM::NUM_PARTITIONS; i++) {
			histogram_to[CONSTANTS_TO::ApplyMask(i)] += histogram_from[i];
		}
		return result;
	}
};

unique_ptr<idx_t[]> RadixPartitioning::ReduceHistogram(const idx_t histogram_from[], idx_t radix_bits_from,
                                                       idx_t radix_bits_to) {
	return DoubleRadixBitsSwitch1<ReduceHistogramFunctor, unique_ptr<idx_t[]>>(radix_bits_from, radix_bits_to,
	                                                                           histogram_from);
}

struct AllocateTempBufFunctor {
	template <idx_t radix_bits>
	static unique_ptr<data_t[]> Operation(idx_t entry_size) {
		using CONSTANTS = RadixPartitioningConstants<radix_bits>;
		return unique_ptr<data_t[]>(new data_t[CONSTANTS::TMP_BUF_SIZE * CONSTANTS::NUM_PARTITIONS * entry_size]);
	}
};

unique_ptr<data_t[]> RadixPartitioning::AllocateTempBuf(idx_t entry_size, idx_t radix_bits) {
	return RadixBitsSwitch<AllocateTempBufFunctor, unique_ptr<data_t[]>>(radix_bits, entry_size);
}

struct PartitionFunctor {
	template <idx_t radix_bits>
	static void Operation(data_ptr_t source_ptr, const data_ptr_t tmp_buf, data_ptr_t dest_ptrs[],
	                      const idx_t entry_size, const idx_t count, const idx_t hash_offset) {
		using CONSTANTS = RadixPartitioningConstants<radix_bits>;

		// Initialize temporal buffer count
		idx_t pos[CONSTANTS::NUM_PARTITIONS];
		for (idx_t idx = 0; idx < CONSTANTS::PARTITIONS; idx++) {
			pos[idx] = idx * CONSTANTS::TMP_BUF_SIZE;
		}
		// Partition
		for (idx_t i = 0; i < count; i++) {
			auto idx = CONSTANTS::ApplyMask(Load<hash_t>(source_ptr + hash_offset));
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

void RadixPartitioning::Partition(data_ptr_t source_ptr, data_ptr_t tmp_buf, data_ptr_t dest_ptrs[], idx_t entry_size,
                                  idx_t count, idx_t hash_offset, idx_t radix_bits) {
	return RadixBitsSwitch<PartitionFunctor, void>(radix_bits, source_ptr, tmp_buf, dest_ptrs, entry_size, count,
	                                               hash_offset);
}

} // namespace duckdb
