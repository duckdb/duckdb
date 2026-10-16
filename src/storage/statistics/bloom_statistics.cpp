#include "duckdb/storage/statistics/bloom_statistics.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

void BloomColumnFilter::InsertHashes(const Vector &hashes_v, idx_t count) noexcept {
	D_ASSERT(hashes_v.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(hashes_v.GetType() == LogicalType::HASH);
	const auto hashes = hashes_v.Values<hash_t>();
	for (idx_t i = 0; i < count; i++) {
		InsertOne(hashes.GetValueUnsafe(i));
	}
}

idx_t BloomColumnFilter::MaybeContains(const Vector &hashes_v, SelectionVector &res_sel, idx_t count) const noexcept {
	D_ASSERT(hashes_v.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(hashes_v.GetType() == LogicalType::HASH);
	const auto hashes = hashes_v.Values<hash_t>();
	idx_t found_count = 0;
	for (idx_t i = 0; i < count; i++) {
		res_sel.set_index(found_count, i);
		found_count += MaybeContainsOne(hashes.GetValueUnsafe(i));
	}
	return found_count;
}

void BloomColumnFilter::InsertOne(hash_t hash) noexcept {
	const idx_t index = hash & (BLOOM_SECTORS - 1);
	words[index] |= GetMask(hash);
}

bool BloomColumnFilter::MaybeContainsOne(hash_t hash) const noexcept {
	const idx_t index = hash & (BLOOM_SECTORS - 1);
	const auto mask = GetMask(hash);
	return (words[index] & mask) == mask;
}

uint64_t BloomColumnFilter::GetMask(hash_t hash) const noexcept {
	constexpr uint64_t SHIFT_MASK = 0x3F3F3F3F3F3F3F3F;
	constexpr idx_t N_BITS = 4;
	const uint64_t shifts = hash & SHIFT_MASK;
	const auto shifts_8 = reinterpret_cast<const uint8_t *>(&shifts);

	uint64_t mask = 0;
	for (idx_t bit_idx = 8 - N_BITS; bit_idx < 8; bit_idx++) {
		const uint8_t bit_pos = shifts_8[bit_idx];
		mask |= (1ULL << bit_pos);
	}
	return mask;
}

void BloomColumnFilter::Serialize(Serializer &serializer) {
	serializer.WriteList(200, "data", words.size(),
	                     [&](Serializer::List &list, idx_t i) { list.WriteElement(words[i]); });
}

void BloomColumnFilter::Deserialize(Deserializer &deserializer) {
	deserializer.ReadList(200, "data",
	                      [&](Deserializer::List &list, idx_t i) { words[i] = list.ReadElement<uint64_t>(); });
}

} // namespace duckdb
