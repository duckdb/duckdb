#include "duckdb/execution/bloom_filter.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

BloomFilter::BloomFilter(size_t expected_cardinality, double desired_false_positive_rate) {
    // Approximate size of the Bloom-filter rounded up to the next 8 byte.
    size_t approx_size = std::ceil(-double(expected_cardinality) * log(desired_false_positive_rate) / 0.48045);
	size_t approx_size_64 = approx_size + (64 - approx_size % 64);
    num_hash_functions = std::ceil(approx_size / expected_cardinality * 0.693147);

    data_buffer.resize(approx_size_64 / 8);
    // TODO: using a duckdb bitstring allows us to inline small bloom-filters into a stack variable.
    // BUT: we might not want to construct small bloom-filters because we have the IN-list optimization, so we just get an unnecessary branch because of that?
    bloom_filter = std::move(duckdb::string_t(data_buffer.data(), data_buffer.size()));
    Bit::SetEmptyBitString(bloom_filter, approx_size_64);  // This is probably not necessary.
}

BloomFilter::~BloomFilter() {
}

inline void BloomFilter::SetBloomBitsForHashes(size_t shift, Vector &hashes, const SelectionVector *rsel, idx_t count) {
    D_ASSERT(hashes.GetType().id() == LogicalType::HASH);
    const size_t bloom_filter_size = Bit::BitLength(bloom_filter);

    if (hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
        auto hash = ConstantVector::GetData<hash_t>(hashes);
		auto bloom_idx = (*hash >> shift) % bloom_filter_size; // TODO: rotation would be a bit nicer because it allows us to generate more values. But C++20 in stdlib.
        Bit::SetBit(bloom_filter, bloom_idx, 0x1);
    } else {
        UnifiedVectorFormat idata;
		hashes.ToUnifiedFormat(count, idata);

        if (!idata.validity.AllValid()) {
            for (idx_t i = 0; i < count; i++) {
                auto ridx = rsel->get_index(i);
			    auto idx = idata.sel->get_index(ridx);
                if (idata.validity.RowIsValid(idx)) {
                    auto hash = UnifiedVectorFormat::GetData<hash_t>(idata)[idx];
                    auto bloom_idx = (hash >> shift) % bloom_filter_size;
                    Bit::SetBit(bloom_filter, bloom_idx, 0x1);
                }
            }
        } else {
            for (idx_t i = 0; i < count; i++) {
                auto ridx = rsel->get_index(i);
			    auto idx = idata.sel->get_index(ridx);
                auto hash = UnifiedVectorFormat::GetData<hash_t>(idata)[idx];
                auto bloom_idx = (hash >> shift) % bloom_filter_size;
                Bit::SetBit(bloom_filter, bloom_idx, 0x1);
            }
        }
    }
}

void BloomFilter::BuildWithPrecomputedHashes(Vector &hashes, const SelectionVector *rsel, idx_t count) {
    // Rotate hash by a couple of bits to produce a new "hash value".
    // With this trick, keys have to be hashed only once.
    for (idx_t i = 0; i < num_hash_functions; i++) {
        auto shift = i * 6;
        SetBloomBitsForHashes(shift, hashes, rsel, count);
    }
}

void BloomFilter::Merge(BloomFilter &other) {
    D_ASSERT(Bit::BitLength(bloom_filter) == Bit::BitLength(other.bloom_filter));
    Bit::BitwiseOr(bloom_filter, other.bloom_filter, bloom_filter);
}

} // namespace duckdb
