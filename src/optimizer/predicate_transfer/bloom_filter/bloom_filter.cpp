#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"

#include <arrow/util/bit_util.h>   // Log2
#include <arrow/util/bitmap_ops.h> // CountSetBits
#include <arrow/util/prefetch.h>   // PREFETCH

#include <iostream>

namespace duckdb {
BloomFilterMasks::BloomFilterMasks() {
	std::seed_seq seed {0, 0, 0, 0, 0, 0, 0, 0};
	std::mt19937 re(seed);
	std::uniform_int_distribution<uint64_t> rd;
	auto random = [&re, &rd](int min_value, int max_value) {
		return min_value + rd(re) % (max_value - min_value + 1);
	};

	memset(masks_, 0, kTotalBytes);

	// Prepare the first mask
	//
	int num_bits_set = static_cast<int>(random(kMinBitsSet, kMaxBitsSet));
	for (int i = 0; i < num_bits_set; ++i) {
		for (;;) {
			int bit_pos = static_cast<int>(random(0, kBitsPerMask - 1));
			if (!arrow::bit_util::GetBit(masks_, bit_pos)) {
				arrow::bit_util::SetBit(masks_, bit_pos);
				break;
			}
		}
	}

	int64_t num_bits_total = kNumMasks + kBitsPerMask - 1;

	// The value num_bits_set will be updated in each iteration of the loop to
	// represent the number of bits set in the entire mask directly preceding the
	// currently processed bit.
	//
	for (int64_t i = kBitsPerMask; i < num_bits_total; ++i) {
		// The value of the lowest bit of the previous mask that will be removed
		// from the current mask as we move to the next position in the bit vector
		// of masks.
		//
		int bit_leaving = arrow::bit_util::GetBit(masks_, i - kBitsPerMask) ? 1 : 0;

		// Next bit has to be 1 because of minimum bits in a mask requirement
		//
		if (bit_leaving == 1 && num_bits_set == kMinBitsSet) {
			arrow::bit_util::SetBit(masks_, i);
			continue;
		}

		// Next bit has to be 0 because of maximum bits in a mask requirement
		//
		if (bit_leaving == 0 && num_bits_set == kMaxBitsSet) {
			continue;
		}

		// Next bit can be random. Use the expected number of bits set in a mask
		// as a probability of 1.
		//
		if (random(0, kBitsPerMask * 2 - 1) < kMinBitsSet + kMaxBitsSet) {
			arrow::bit_util::SetBit(masks_, i);
			if (bit_leaving == 0) {
				++num_bits_set;
			}
		} else {
			if (bit_leaving == 1) {
				--num_bits_set;
			}
		}
	}
}

BloomFilterMasks BlockedBloomFilter::masks_;

arrow::Status BlockedBloomFilter::CreateEmpty(int64_t num_rows_to_insert, arrow::MemoryPool *pool) {
	// Compute the size
	//
	constexpr int64_t min_num_bits_per_key = 8;
	// constexpr int64_t min_num_bits_per_key = 16;
	constexpr int64_t min_num_bits = 512;
	int64_t desired_num_bits = std::max(min_num_bits, num_rows_to_insert * min_num_bits_per_key);
	int log_num_bits = arrow::bit_util::Log2(desired_num_bits);

	log_num_blocks_ = log_num_bits - 6;
	num_blocks_ = 1ULL << log_num_blocks_;

	// Allocate and zero out bit vector
	//
	int64_t buffer_size = num_blocks_ * sizeof(uint64_t);
	ARROW_ASSIGN_OR_RAISE(buf_, AllocateBuffer(buffer_size, pool));

	// blocks_ = reinterpret_cast<uint64_t*>(buf_->mutable_data());
	blocks_ = reinterpret_cast<std::atomic<uint64_t> *>(buf_->mutable_data());

	memset(blocks_, 0, buffer_size);
	return arrow::Status::OK();
}

template <typename T>
void BlockedBloomFilter::InsertImp(int64_t num_rows, const T *hashes) {
	for (int64_t i = 0; i < num_rows; ++i) {
		Insert(hashes[i]);
	}
}

void BlockedBloomFilter::Insert(int64_t hardware_flags, int64_t num_rows, const uint64_t *hashes) {
	int64_t num_processed = 0;
	if (hardware_flags & arrow::internal::CpuInfo::AVX2) {
		num_processed = Insert_avx2(num_rows, hashes);
	}
	InsertImp(num_rows - num_processed, hashes + num_processed);
}

static inline int trailingzeroes(uint64_t input_num) {
#ifdef __BMI2__
	return _tzcnt_u64(input_num);
#else
	return __builtin_ctzll(input_num);
#endif
}

inline void basic_decoder(SelectionVector &sel, idx_t &result_count, uint32_t idx, uint64_t bits) {
	while (bits != 0) {
		uint32_t value = static_cast<uint32_t>(idx) + trailingzeroes(bits);
		sel.set_index(result_count, value);
		bits = bits & (bits - 1);
		result_count++;
	}
}

void BlockedBloomFilter::FindImp(int64_t num_rows, int64_t num_preprocessed, const uint64_t *hashes,
                                 SelectionVector &sel, idx_t &result_count, bool enable_prefetch) const {
	int64_t num_processed = 0;
	if (enable_prefetch && UsePrefetch()) {
		constexpr int kPrefetchIterations = 16;
		for (int64_t i = 0; i < num_rows - kPrefetchIterations; ++i) {
			ARROW_PREFETCH(blocks_ + block_id(hashes[i + kPrefetchIterations]));
			bool result = Find(hashes[i]);
			sel.set_index(result_count, i);
			result_count += result;
		}
		num_processed = num_rows - kPrefetchIterations;
	}

	if (num_processed < 0) {
		num_processed = 0;
	}
	for (int64_t i = num_processed; i < num_rows; i++) {
		bool result = Find(hashes[i]);
		sel.set_index(result_count, i + num_preprocessed);
		result_count += result;
	}
}

void BlockedBloomFilter::Find(int64_t hardware_flags, int64_t num_rows, const uint64_t *hashes, SelectionVector &sel,
                              idx_t &result_count, bool enable_prefetch) const {
	int64_t num_processed = 0;

	if (!(enable_prefetch && UsePrefetch()) && (hardware_flags & arrow::internal::CpuInfo::AVX2)) {
		uint8_t *result_bit_vector = new uint8_t[num_rows / 8 + 8];
		memset(result_bit_vector, 0, num_rows / 8 + 8);
		num_processed = Find_avx2(num_rows, hashes, result_bit_vector);
		for (uint32_t i = 0; i < num_rows / 64 + 1; i++) {
			uint64_t bits = ((uint64_t *)result_bit_vector)[i];
			basic_decoder(sel, result_count, 64 * i, bits);
		}
		num_processed -= (num_processed % 8);
		delete[] result_bit_vector;
	}

	ARROW_DCHECK(num_processed % 8 == 0);
	FindImp(num_rows - num_processed, num_processed, hashes + num_processed, sel, result_count, enable_prefetch);
}

int BlockedBloomFilter::NumHashBitsUsed() const {
	constexpr int num_bits_for_mask = (BloomFilterMasks::kLogNumMasks + 6);
	int num_bits_for_block = log_num_blocks();
	return num_bits_for_mask + num_bits_for_block;
}

bool BlockedBloomFilter::IsSameAs(const BlockedBloomFilter *other) const {
	if (log_num_blocks_ != other->log_num_blocks_ || num_blocks_ != other->num_blocks_) {
		return false;
	}
	if (memcmp(blocks_, other->blocks_, num_blocks_ * sizeof(uint64_t)) != 0) {
		return false;
	}
	return true;
}

int64_t BlockedBloomFilter::NumBitsSet() const {
	return arrow::internal::CountSetBits(reinterpret_cast<const uint8_t *>(blocks_), 0, (1LL << log_num_blocks()) * 64);
}

arrow::Status BloomFilterBuilderParallel::Begin(size_t num_threads, int64_t hardware_flags, arrow::MemoryPool *pool,
                                                int64_t num_rows, int64_t num_batches,
                                                BlockedBloomFilter *build_target) {
	hardware_flags_ = hardware_flags;
	build_target_ = build_target;
	total_row_nums_ = num_rows;

	constexpr int kMaxLogNumPrtns = 8;
	log_num_prtns_ = std::min(kMaxLogNumPrtns, arrow::bit_util::Log2(num_threads));

	thread_local_states_.resize(num_threads);

	RETURN_NOT_OK(build_target->CreateEmpty(num_rows, pool));

	return arrow::Status::OK();
}

arrow::Status BloomFilterBuilderParallel::PushNextBatch(size_t thread_id, int64_t num_rows, const uint64_t *hashes) {
	PushNextBatchImp(thread_id, num_rows, hashes);
	return arrow::Status::OK();
}

vector<idx_t> BloomFilterBuilderParallel::BuiltCols() {
	return build_target_->BoundColsBuilt;
}

void BloomFilterBuilderParallel::PushNextBatchImp(size_t thread_id, int64_t num_rows, const uint64_t *hashes) {
	// Partition IDs are calculated using the higher bits of the block ID.  This
	// ensures that each block is contained entirely within a partition and prevents
	// concurrent access to a block.
	constexpr int kLogBlocksKeptTogether = 7;
	constexpr int kPrtnIdBitOffset = BloomFilterMasks::kLogNumMasks + 6 + kLogBlocksKeptTogether;

	const int log_num_prtns_max = std::max(0, build_target_->log_num_blocks() - kLogBlocksKeptTogether);
	const int log_num_prtns_mod = std::min(log_num_prtns_, log_num_prtns_max);
	int num_prtns = 1 << log_num_prtns_mod;

	ThreadLocalState &local_state = thread_local_states_[thread_id];
	local_state.partition_ranges.resize(num_prtns + 1);
	local_state.partitioned_hashes_64.resize(num_rows);
	local_state.unprocessed_partition_ids.resize(num_prtns);
	uint16_t *partition_ranges = local_state.partition_ranges.data();
	uint64_t *partitioned_hashes = local_state.partitioned_hashes_64.data();
	int *unprocessed_partition_ids = local_state.unprocessed_partition_ids.data();

	PartitionSort::Eval(
	    num_rows, num_prtns, partition_ranges,
	    [=](int64_t row_id) { return (hashes[row_id] >> (kPrtnIdBitOffset)) & (num_prtns - 1); },
	    [=](int64_t row_id, int output_pos) { partitioned_hashes[output_pos] = hashes[row_id]; });

	int num_unprocessed_partitions = 0;
	for (int i = 0; i < num_prtns; ++i) {
		bool is_prtn_empty = (partition_ranges[i + 1] == partition_ranges[i]);
		if (!is_prtn_empty) {
			unprocessed_partition_ids[num_unprocessed_partitions++] = i;
		}
	}
	while (num_unprocessed_partitions > 0) {
		int locked_prtn_id;
		int locked_prtn_id_pos;
		locked_prtn_id_pos = 0;
		locked_prtn_id = unprocessed_partition_ids[locked_prtn_id_pos];
		// prtn_locks_.AcquirePartitionLock(thread_id, num_unprocessed_partitions, unprocessed_partition_ids, false, -1,
		// &locked_prtn_id, &locked_prtn_id_pos);
		build_target_->Insert(
		    // local_state.local_bf->Insert(
		    hardware_flags_, partition_ranges[locked_prtn_id + 1] - partition_ranges[locked_prtn_id],
		    partitioned_hashes + partition_ranges[locked_prtn_id]);
		// prtn_locks_.ReleasePartitionLock(locked_prtn_id);
		if (locked_prtn_id_pos < num_unprocessed_partitions - 1) {
			unprocessed_partition_ids[locked_prtn_id_pos] = unprocessed_partition_ids[num_unprocessed_partitions - 1];
		}
		--num_unprocessed_partitions;
	}
}

void BloomFilterBuilderParallel::CleanUp() {
	thread_local_states_.clear();
}

void BloomFilterBuilderParallel::Merge() {
}
} // namespace duckdb
