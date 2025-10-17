

#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

void CacheSectorizedBloomFilter::Initialize(ClientContext &context_p, idx_t est_num_rows) {
	context = &context_p;
	buffer_manager = &BufferManager::GetBufferManager(*context);

	const idx_t min_bits = std::max<idx_t>(MIN_NUM_BITS, est_num_rows * MIN_NUM_BITS_PER_KEY);
	num_sectors = std::min(NextPowerOfTwo(min_bits) >> LOG_SECTOR_SIZE, MAX_NUM_SECTORS);
	num_sectors_log = static_cast<uint32_t>(std::log2(num_sectors));

	buf_ = buffer_manager->GetBufferAllocator().Allocate(64 + num_sectors * sizeof(uint32_t));
	// make sure blocks is a 64-byte aligned pointer, i.e., cache-line aligned
	blocks = reinterpret_cast<uint32_t *>((64ULL + reinterpret_cast<uint64_t>(buf_.get())) & ~63ULL);
	std::fill_n(blocks, num_sectors, 0);

	initialized = true;
	active = true;
}

uint32_t CacheSectorizedBloomFilter::GetMask1(const uint32_t key_lo) {
	// 3 bits in key_lo
	return (1u << ((key_lo >> 17) & 31)) | (1u << ((key_lo >> 22) & 31)) | (1u << ((key_lo >> 27) & 31));
}

uint32_t CacheSectorizedBloomFilter::GetMask2(const uint32_t key_hi) {
	// 4 bits in key_hi
	return (1u << ((key_hi >> 12) & 31)) | (1u << ((key_hi >> 17) & 31)) | (1u << ((key_hi >> 22) & 31)) |
		   (1u << ((key_hi >> 27) & 31));
}

uint32_t CacheSectorizedBloomFilter::GetSector1(const uint32_t key_lo, const uint32_t key_hi) const {
	// block: 13 bits in key_lo and 9 bits in key_hi
	// sector 1: 4 bits in key_lo
	return ((key_lo & ((1 << 17) - 1)) + ((key_hi << 14) & (((1 << 9) - 1) << 17))) & (num_sectors - 1);
}

uint32_t CacheSectorizedBloomFilter::GetSector2(const uint32_t key_hi, const uint32_t block1) const {
	// sector 2: 3 bits in key_hi
	return block1 ^ (8 + (key_hi & 7));
}

void CacheSectorizedBloomFilter::InsertOne(const uint32_t key_lo, const uint32_t key_hi, uint32_t *__restrict bf) const {
	const uint32_t sector1 = GetSector1(key_lo, key_hi);
	const uint32_t mask1 = GetMask1(key_lo);
	const uint32_t sector2 = GetSector2(key_hi, sector1);
	const uint32_t mask2 = GetMask2(key_hi);

	// Perform atomic OR operation on the bf array elements using std::atomic
	std::atomic<uint32_t> &atomic_bf1 = *reinterpret_cast<std::atomic<uint32_t> *>(&bf[sector1]);
	std::atomic<uint32_t> &atomic_bf2 = *reinterpret_cast<std::atomic<uint32_t> *>(&bf[sector2]);

	atomic_bf1.fetch_or(mask1, std::memory_order_relaxed);
	atomic_bf2.fetch_or(mask2, std::memory_order_relaxed);
}

bool CacheSectorizedBloomFilter::LookupOne(uint32_t key_lo, uint32_t key_hi, const uint32_t *__restrict bf) const {
	const uint32_t sector1 = GetSector1(key_lo, key_hi);
	const uint32_t mask1 = GetMask1(key_lo);
	const uint32_t sector2 = GetSector2(key_hi, sector1);
	const uint32_t mask2 = GetMask2(key_hi);
	return ((bf[sector1] & mask1) == mask1) & ((bf[sector2] & mask2) == mask2);
}

string BloomFilter::ToString(const string &column_name) const {
	if (filter.IsInitialized()) {
		return column_name + " IN BF(" + key_column_name + ")";
	} else {
		return "True";
	}
}

unique_ptr<Expression> BloomFilter::ToExpression(const Expression &column) const {
	auto bound_constant = make_uniq<BoundConstantExpression>(Value(true));
	return std::move(bound_constant); // todo: I can't really have an expression for this, so this is a hack
}

idx_t __attribute__((noinline))
CacheSectorizedBloomFilter::LookupHashes(const Vector &hashes, SelectionVector &result_sel, const idx_t count) const {
	D_ASSERT(hashes.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(hashes.GetType() == LogicalType::HASH);
	return BloomFilterLookup(reinterpret_cast<hash_t *>(hashes.GetData()), blocks, result_sel, count);
}

bool CacheSectorizedBloomFilter::LookupHash(hash_t hash) const {
	// Reinterpret the address of a value as a pointer to uint32_t
	const uint32_t *parts = reinterpret_cast<uint32_t *>(&hash);

	const uint32_t lower = parts[0];
	const uint32_t higher = parts[1];

	return LookupOne(lower, higher, blocks);
}

void CacheSectorizedBloomFilter::InsertHashes(const Vector &hashes, const idx_t count) const {
	BloomFilterInsert(count, reinterpret_cast<uint64_t *>(hashes.GetData()), blocks);
}

} // namespace duckdb
