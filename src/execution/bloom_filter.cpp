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

    bloom_filter = std::move(duckdb::string_t(approx_size_64 / 8));
    Bit::SetEmptyBitString(bloom_filter, approx_size_64);
}

BloomFilter::~BloomFilter() {
}

static inline void HashesToBloomFilterIndexes(const Vector &hashes, Vector& indexes, size_t shift, size_t bloom_filter_size, const SelectionVector *rsel, idx_t count) {
    D_ASSERT(hashes.GetType().id() == LogicalType::HASH);

    if (hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
        indexes.SetVectorType(VectorType::CONSTANT_VECTOR);

        auto ldata = ConstantVector::GetData<hash_t>(hashes);
		auto indexes_data = ConstantVector::GetData<uint32_t>(indexes);
		*indexes_data = (*ldata >> shift) % bloom_filter_size;
    } else {
        indexes.SetVectorType(VectorType::FLAT_VECTOR);

        UnifiedVectorFormat idata;
		indexes.ToUnifiedFormat(count, idata);

        if (!idata.validity.AllValid()) {
            for (idx_t i = 0; i < count; i++) {
                auto ridx = rsel->get_index(i);
			    auto idx = idata.sel->get_index(ridx);
                if (idata.validity.RowIsValid(idx)) {
                    FlatVector::GetData<uint32_t>(indexes)[ridx] = UnifiedVectorFormat::GetData<hash_t>(idata)[idx];
                }
            }
        } else {
            for (idx_t i = 0; i < count; i++) {
                auto ridx = rsel->get_index(i);
			    auto idx = idata.sel->get_index(ridx);
                FlatVector::GetData<uint32_t>(indexes)[ridx] = UnifiedVectorFormat::GetData<hash_t>(idata)[idx];
            }
        }
    }
}

static inline void SetBloomFilterBitsAtIndexes(bitstring_t &bloom_filter, Vector &indexes, const SelectionVector *rsel, idx_t count) {
    D_ASSERT(indexes.GetType().id() == LogicalType::UINTEGER);

    if (indexes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
        if (!ConstantVector::IsNull(indexes)) {
            auto idx = ConstantVector::GetData<uint32_t>(indexes);
            Bit::SetBit(bloom_filter, *idx, 0x1);
        }
    } else {
        UnifiedVectorFormat idata;
		indexes.ToUnifiedFormat(count, idata);
        
        if (!idata.validity.AllValid()) {
            for (idx_t i = 0; i < count; i++) {
                auto ridx = rsel->get_index(i);
			    auto idx = idata.sel->get_index(ridx);
                if (idata.validity.RowIsValid(idx)) {
                    Bit::SetBit(bloom_filter, UnifiedVectorFormat::GetData<uint32_t>(idata)[idx], 0x1);
                }
            }
        } else {
            for (idx_t i = 0; i < count; i++) {
                auto ridx = rsel->get_index(i);
			    auto idx = idata.sel->get_index(ridx);
                Bit::SetBit(bloom_filter, UnifiedVectorFormat::GetData<uint32_t>(idata)[idx], 0x1);
            }
        }
    }
}

void BloomFilter::BuildWithPrecomputedHashes(Vector &hashes, const SelectionVector *rsel, idx_t count) {
    for (idx_t i = 0; i < num_hash_functions; i++) {
        auto shift = i * 6;
        Vector bloom_bit_idxs(LogicalType::UINTEGER);
        
        HashesToBloomFilterIndexes(hashes, bloom_bit_idxs, shift, Bit::BitLength(bloom_filter), rsel, count);
        SetBloomFilterBitsAtIndexes(bloom_filter, bloom_bit_idxs, rsel, count);
    }
}

void BloomFilter::Merge(BloomFilter &other) {
    D_ASSERT(Bit::BitLength(bloom_filter) == Bit::BitLength(other.bloom_filter));
    Bit::BitwiseOr(bloom_filter, other.bloom_filter, bloom_filter);
}

} // namespace duckdb
