#pragma once

#include <immintrin.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <unordered_map>

#include "arrow/acero/util.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/optimizer/predicate_transfer/bloom_filter/partition_util.hpp"

namespace duckdb {

template <typename T>
inline T SafeLoadAs(const uint8_t *unaligned) {
	std::remove_const_t<T> ret;
	std::memcpy(&ret, unaligned, sizeof(T));
	return ret;
}

// A set of pre-generated bit masks from a 64-bit word.
//
// It is used to map selected bits of hash to a bit mask that will be used in
// a Bloom filter.
//
// These bit masks need to look random and need to have a similar fractions of
// bits set in order for a Bloom filter to have a low false positives rate.
//
struct BloomFilterMasks {
	// Generate all masks as a single bit vector. Each bit offset in this bit
	// vector corresponds to a single mask.
	// In each consecutive kBitsPerMask bits, there must be between
	// kMinBitsSet and kMaxBitsSet bits set.
	//
	BloomFilterMasks();

	inline uint64_t mask(int bit_offset) {
#if ARROW_LITTLE_ENDIAN
		return (SafeLoadAs<uint64_t>(masks_ + bit_offset / 8) >> (bit_offset % 8)) & kFullMask;
#else
		return (BYTESWAP(SafeLoadAs<uint64_t>(masks_ + bit_offset / 8)) >> (bit_offset % 8)) & kFullMask;
#endif
	}

	// Masks are 57 bits long because then they can be accessed at an
	// arbitrary bit offset using a single unaligned 64-bit load instruction.
	//
	static constexpr int kBitsPerMask = 57;
	static constexpr uint64_t kFullMask = (1ULL << kBitsPerMask) - 1;

	// Minimum and maximum number of bits set in each mask.
	// This constraint is enforced when generating the bit masks.
	// Values should be close to each other and chosen as to minimize a Bloom
	// filter false positives rate.
	//
	static constexpr int kMinBitsSet = 4;
	static constexpr int kMaxBitsSet = 5;

	// Number of generated masks.
	// Having more masks to choose will improve false positives rate of Bloom
	// filter but will also use more memory, which may lead to more CPU cache
	// misses.
	// The chosen value results in using only a few cache-lines for mask lookups,
	// while providing a good variety of available bit masks.
	//
	static constexpr int kLogNumMasks = 10;
	static constexpr int kNumMasks = 1 << kLogNumMasks;

	// Data of masks. Masks are stored in a single bit vector. Nth mask is
	// kBitsPerMask bits starting at bit offset N.
	//
	static constexpr int kTotalBytes = (kNumMasks + 64) / 8;
	uint8_t masks_[kTotalBytes];
};

// A variant of a blocked Bloom filter implementation.
// A Bloom filter is a data structure that provides approximate membership test
// functionality based only on the hash of the key. Membership test may return
// false positives but not false negatives. Approximation of the result allows
// in general case (for arbitrary data types of keys) to save on both memory and
// lookup cost compared to the accurate membership test.
// The accurate test may sometimes still be cheaper for a specific data types
// and inputs, e.g. integers from a small range.
//
// This blocked Bloom filter is optimized for use in hash joins, to achieve a
// good balance between the size of the filter, the cost of its building and
// querying and the rate of false positives.
//
class BlockedBloomFilter {
	friend class BloomFilterBuilder_SingleThreaded;
	friend class BloomFilterBuilder_Parallel;

public:
	BlockedBloomFilter()
	    : private_masks_(nullptr), log_num_blocks_(0), num_blocks_(0), blocks_(nullptr), use_64bit_hashes_(true),
	      Used_(false) {
	}
	BlockedBloomFilter(int log_num_blocks, bool use_64bit_hashes)
	    : log_num_blocks_(log_num_blocks), num_blocks_(1ULL << log_num_blocks_), use_64bit_hashes_(use_64bit_hashes),
	      Used_(false) {
	}

	void AddColumnBindingApplied(ColumnBinding column_binding) {
		column_bindings_applied_.emplace_back(column_binding);
	}

	void AddColumnBindingBuilt(ColumnBinding column_binding) {
		column_bindings_built_.emplace_back(column_binding);
	}

	inline bool Find(uint64_t hash) const {
		uint64_t m = mask(hash);
		uint64_t b = blocks_[block_id(hash)];
		return (b & m) == m;
	}

	// Uses SIMD if available for smaller Bloom filters.
	// Uses memory prefetching for larger Bloom filters.
	//
	void Find(int64_t hardware_flags, int64_t num_rows, const uint64_t *hashes, SelectionVector &sel,
	          idx_t &result_count, bool enable_prefetch = true) const;

	vector<ColumnBinding> GetColApplied() {
		return column_bindings_applied_;
	}

	vector<ColumnBinding> GetColBuilt() {
		return column_bindings_built_;
	}

	int log_num_blocks() const {
		return log_num_blocks_;
	}

	bool use_64bit_hashes() const {
		return use_64bit_hashes_;
	}

	int NumHashBitsUsed() const;

	bool IsSameAs(const BlockedBloomFilter *other) const;

	bool isEmpty() {
		return blocks_ == nullptr;
	}

	int64_t NumBitsSet() const;

	// Folding of a block Bloom filter after the initial version
	// has been built.
	//
	// One of the parameters for creation of Bloom filter is the number
	// of bits allocated for it. The more bits allocated, the lower the
	// probability of false positives. A good heuristic is to aim for
	// half of the bits set in the constructed Bloom filter. This should
	// result in a good trade off between size (and following cost of
	// memory accesses) and false positives rate.
	//
	// There might have been many duplicate keys in the input provided
	// to Bloom filter builder. In that case the resulting bit vector
	// would be more sparse then originally intended. It is possible to
	// easily correct that and cut in half the size of Bloom filter
	// after it has already been constructed. The process to do that is
	// approximately equal to OR-ing bits from upper and lower half (the
	// way we address these bits when inserting or querying a hash makes
	// such folding in half possible).
	//
	// We will keep folding as long as the fraction of bits set is less
	// than 1/4. The resulting bit vector density should be in the [1/4,
	// 1/2) range.
	//
	void Fold();

	bool isUsed() {
		return Used_;
	}

	void setUsed() {
		Used_ = true;
	}

	// Num bits used per hash value
	static constexpr int64_t kMinNumBitsPerKey = 8;
	// static constexpr int64_t kMinNumBitsPerKey = 16;

	// Maximum number of actually used blocks for 32-bit hashes, given num bits used to get mask.
	// When we want to use more blocks, 64-bit hashes are required.
	// It can still work if we try to use more blocks than this bound for 32-bit hashes, but will result in a much
	// worse false positive rate.
	static constexpr int64_t kNumBitsBlocksUsedBy32Bit = 32 - (BloomFilterMasks::kLogNumMasks + 6);
	static constexpr int64_t kNumBlocksUsedBy32Bit = 1 << kNumBitsBlocksUsedBy32Bit;
	static constexpr int64_t kMaxNumRowsFor32Bit = kNumBlocksUsedBy32Bit * 64 / kMinNumBitsPerKey;

	// The columns applied this BF
	vector<ColumnBinding> column_bindings_applied_;

	// The columns build this BF
	vector<ColumnBinding> column_bindings_built_;

	vector<idx_t> BoundColsApplied;

	vector<idx_t> BoundColsBuilt;

	arrow::Status CreateEmpty(int64_t num_rows_to_insert, arrow::MemoryPool *pool);

	void Insert(int64_t hardware_flags, int64_t num_rows, const uint64_t *hashes);

private:
	inline void Insert(uint64_t hash) {
		uint64_t m = mask(hash);
		// uint64_t &b = blocks_[block_id(hash)];
		// b |= m;
		std::atomic<uint64_t> &b = blocks_[block_id(hash)];
		b.fetch_or(m);
	}

	inline uint64_t mask(uint64_t hash) const {
		// The lowest bits of hash are used to pick mask index.
		//
		int mask_id = static_cast<int>(hash & (BloomFilterMasks::kNumMasks - 1));
		uint64_t result = masks_.mask(mask_id);

		// The next set of hash bits is used to pick the amount of bit
		// rotation of the mask.
		//
		int rotation = (hash >> BloomFilterMasks::kLogNumMasks) & 63;
		result = ROTL64(result, rotation);

		return result;
	}

	inline int64_t block_id(uint64_t hash) const {
		// The next set of hash bits following the bits used to select a
		// mask is used to pick block id (index of 64-bit word in a bit
		// vector).
		//
		return (hash >> (BloomFilterMasks::kLogNumMasks + 6)) & (num_blocks_ - 1);
	}

	template <typename T>
	inline void InsertImp(int64_t num_rows, const T *hashes);

	inline void FindImp(int64_t num_rows, int64_t num_preprocessed, const uint64_t *hashes, SelectionVector &sel,
	                    idx_t &result_count, bool enable_prefetch) const;

	void SingleFold(int num_folds);

	inline __m256i mask_avx2(__m256i hash) const;
	inline __m256i block_id_avx2(__m256i hash) const;
	int64_t Insert_avx2(int64_t num_rows, const uint32_t *hashes);
	int64_t Insert_avx2(int64_t num_rows, const uint64_t *hashes);
	template <typename T>
	int64_t InsertImp_avx2(int64_t num_rows, const T *hashes);
	int64_t Find_avx2(int64_t num_rows, const uint64_t *hashes, uint8_t *result_bit_vector) const;
	int64_t FindImp_avx2(int64_t num_rows, const uint64_t *hashes, uint8_t *result_bit_vector) const;

	bool UsePrefetch() const {
		return num_blocks_ * sizeof(uint64_t) > kPrefetchLimitBytes;
	}

	void SetBuf(const std::shared_ptr<arrow::Buffer> &buf) {
		buf_ = buf;
		// blocks_ = reinterpret_cast<uint64_t*>(const_cast<uint8_t*>(buf_->data()));
		blocks_ = reinterpret_cast<std::atomic<uint64_t> *>(const_cast<uint8_t *>(buf_->data()));
	}

	static constexpr int64_t kPrefetchLimitBytes = 256 * 1024;

	static BloomFilterMasks masks_;

	// Used when receiving bloom filters from other nodes, where
	// the same masks should be used instead of the static one.
	std::shared_ptr<BloomFilterMasks> private_masks_;

	// Total number of bits used by block Bloom filter must be a power
	// of 2.
	//
	int log_num_blocks_;
	int64_t num_blocks_;

	// Whether to use 64-bit hashes as input values.
	bool use_64bit_hashes_;

	// Buffer allocated to store an array of power of 2 64-bit blocks.
	std::shared_ptr<arrow::Buffer> buf_;

	// Pointer to mutable data owned by Buffer
	// uint64_t* blocks_ = nullptr;
	std::atomic<uint64_t> *blocks_;

	bool Used_;
};

// We have two separate implementations of building a Bloom filter, multi-threaded and
// single-threaded.
//
// Single threaded version is useful in two ways:
// a) It allows to verify parallel implementation in tests (the single threaded one is
// simpler and can be used as the source of truth).
// b) It is preferred for small and medium size Bloom filters, because it skips extra
// synchronization related steps from parallel variant (partitioning and taking locks).
//
enum class BloomFilterBuildStrategy {
	SINGLE_THREADED = 0,
	PARALLEL = 1,
};

class BloomFilterBuilder {
public:
	virtual ~BloomFilterBuilder() = default;
	virtual arrow::Status Begin(size_t num_threads, int64_t hardware_flags, arrow::MemoryPool *pool, int64_t num_rows,
	                            int64_t num_batches, BlockedBloomFilter *build_target) = 0;

	virtual int64_t num_tasks() const {
		return 0;
	}
	virtual arrow::Status PushNextBatch(size_t thread_index, int64_t num_rows, const uint64_t *hashes) = 0;

	virtual vector<idx_t> BuiltCols() = 0;

	virtual void CleanUp() {
	}

	virtual void Merge() {
	}
};

class BloomFilterBuilder_SingleThreaded : public BloomFilterBuilder {
public:
	arrow::Status Begin(size_t num_threads, int64_t hardware_flags, arrow::MemoryPool *pool, int64_t num_rows,
	                    int64_t num_batches, BlockedBloomFilter *build_target) override;

	arrow::Status PushNextBatch(size_t /*thread_index*/, int64_t num_rows, const uint64_t *hashes) override;

	vector<idx_t> BuiltCols() override;

private:
	void PushNextBatchImp(int64_t num_rows, const uint64_t *hashes);

	int64_t hardware_flags_;
	BlockedBloomFilter *build_target_;
};

class ARROW_ACERO_EXPORT BloomFilterBuilder_Parallel : public BloomFilterBuilder {
public:
	arrow::Status Begin(size_t num_threads, int64_t hardware_flags, arrow::MemoryPool *pool, int64_t num_rows,
	                    int64_t num_batches, BlockedBloomFilter *build_target) override;

	arrow::Status PushNextBatch(size_t thread_id, int64_t num_rows, const uint64_t *hashes) override;

	void CleanUp() override;

	void Merge() override;

	vector<idx_t> BuiltCols() override;

private:
	void PushNextBatchImp(size_t thread_id, int64_t num_rows, const uint64_t *hashes);

	int64_t total_row_nums_;
	int64_t hardware_flags_;
	BlockedBloomFilter *build_target_;
	int log_num_prtns_;
	struct ThreadLocalState {
		std::vector<uint32_t> partitioned_hashes_32;
		std::vector<uint64_t> partitioned_hashes_64;
		std::vector<uint16_t> partition_ranges;
		std::vector<int> unprocessed_partition_ids;
		shared_ptr<BlockedBloomFilter> local_bf;
	};
	std::vector<ThreadLocalState> thread_local_states_;
	PartitionLocks prtn_locks_;
};
} // namespace duckdb
