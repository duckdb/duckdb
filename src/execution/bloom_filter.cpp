#include "duckdb/execution/bloom_filter.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include <iostream>

namespace duckdb {

BloomFilter::BloomFilter(size_t expected_cardinality, double desired_false_positive_rate) : num_inserted_rows(0), probing_started(false) {
    // Approximate size of the Bloom-filter rounded up to the next 8 byte.
    size_t approx_size = std::ceil(-double(expected_cardinality) * log(desired_false_positive_rate) / 0.48045);
	bloom_filter_size = approx_size + (64 - approx_size % 64);
    num_hash_functions = std::ceil(approx_size / expected_cardinality * 0.693147);

    bloom_data_buffer.resize(bloom_filter_size / 64, 0);
    bloom_filter.Initialize(bloom_data_buffer.data(), bloom_filter_size);
}

BloomFilter::~BloomFilter() {
}

inline size_t HashToIndex(hash_t hash, size_t bloom_filter_size, size_t i) {
    return (hash >> (i * 1)) % bloom_filter_size;  // TODO: rotation would be a bit nicer because it allows us to generate more values. But C++20 in stdlib.
}

inline void BloomFilter::SetBloomBitsForHashes(size_t fni, Vector &hashes, const SelectionVector &rsel, idx_t count) {
    D_ASSERT(hashes.GetType().id() == LogicalType::HASH);
    D_ASSERT(probing_started == false);

    if (hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
        auto hash = ConstantVector::GetData<hash_t>(hashes);
		auto bloom_idx = HashToIndex(*hash, bloom_filter_size, fni);
        bloom_filter.SetValid(bloom_idx);
    } else {
        UnifiedVectorFormat u_hashes;
		hashes.ToUnifiedFormat(count, u_hashes);

        if (!u_hashes.validity.AllValid()) {
            for (idx_t i = 0; i < count; i++) {
                auto key_idx = rsel.get_index(i);
			    auto hash_idx = u_hashes.sel->get_index(key_idx);
                if (u_hashes.validity.RowIsValid(hash_idx)) {
                    auto hash = UnifiedVectorFormat::GetData<hash_t>(u_hashes)[hash_idx];
                    auto bloom_idx = HashToIndex(hash, bloom_filter_size, fni);
                    bloom_filter.SetValid(bloom_idx);
                }
            }
        } else {
            for (idx_t i = 0; i < count; i++) {
                auto key_idx = rsel.get_index(i);
			    auto hash_idx = u_hashes.sel->get_index(key_idx);
                auto* hashes = UnifiedVectorFormat::GetData<hash_t>(u_hashes);
                auto hash = hashes[hash_idx];
                auto bloom_idx = HashToIndex(hash, bloom_filter_size, fni);
                bloom_filter.SetValid(bloom_idx);
            }
        }
    }
}

void BloomFilter::BuildWithPrecomputedHashes(Vector &hashes, const SelectionVector &rsel, idx_t count) {
    // Rotate hash by a couple of bits to produce a new "hash value".
    // With this trick, keys have to be hashed only once.
    for (idx_t i = 0; i < num_hash_functions; i++) {
        SetBloomBitsForHashes(i, hashes, rsel, count);
        std::cout << "Bloom-filter after inserting " << count << " rows in round " << i << " of " << num_hash_functions << ": " << Bit::ToString(bloom_filter) << std::endl;
    }
    num_inserted_rows += count;
}

inline size_t BloomFilter::ProbeInternal(size_t fni, Vector &hashes, const SelectionVector *&current_sel, idx_t current_sel_count, SelectionVector &sel) {
    D_ASSERT(hashes.GetType().id() == LogicalType::HASH);
    D_ASSERT(current_sel_count > 0); // Should be handled before

    current_sel = FlatVector::IncrementalSelectionVector();

    if (hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
        auto hash = ConstantVector::GetData<hash_t>(hashes);
		auto bloom_idx = HashToIndex(*hash, bloom_filter_size, fni);

        if (bloom_filter.RowIsValid(bloom_idx)) {
            // All constant elements match. No need to modify the selection vector.
            return current_sel_count;
        } else {
            // TODO: we need to set the whole 'out' vector to zero?
            return 0;
        }
    } else {
        UnifiedVectorFormat u_hashes;
		hashes.ToUnifiedFormat(current_sel_count, u_hashes);

        if (!u_hashes.validity.AllValid()) {
            size_t sel_out_idx = 0;
            for (idx_t i = 0; i < current_sel_count; i++) {
                // TODO: We can skip this row if it was already removed by a previous iteration
                auto key_idx = current_sel->get_index(i);
			    auto hash_idx = u_hashes.sel->get_index(key_idx);
                if (u_hashes.validity.RowIsValid(hash_idx)) {
                    auto* hashes = UnifiedVectorFormat::GetData<hash_t>(u_hashes);
                    auto hash = hashes[hash_idx];
                    auto bloom_idx = HashToIndex(hash, bloom_filter_size, fni);
                    if (bloom_filter.RowIsValid(bloom_idx)) {
                        // Bit is set in Bloom-filter. We keep the entry for now.
                        sel.set_index(sel_out_idx++, key_idx);
                    }
                }
            }
            current_sel = &sel;
            return sel_out_idx;
        } else {
            size_t sel_out_idx = 0;
            for (idx_t i = 0; i < current_sel_count; i++) {
                auto key_idx = current_sel->get_index(i);
			    auto hash_idx = u_hashes.sel->get_index(key_idx);
                auto* hashes = UnifiedVectorFormat::GetData<hash_t>(u_hashes);
                auto hash = hashes[hash_idx];
                auto bloom_idx = HashToIndex(hash, bloom_filter_size, fni);
                if (bloom_filter.RowIsValid(bloom_idx)) {
                    // Bit is set in Bloom-filter. We keep the entry for now.
                    sel.set_index(sel_out_idx++, key_idx);
                }
            }
            current_sel = &sel;
            return sel_out_idx;
        }
    }
}


size_t BloomFilter::Probe(DataChunk &keys, const SelectionVector *&current_sel, idx_t count, SelectionVector sel, optional_ptr<Vector> precomputed_hashes) {
    // The code currently assumes that we have precomputed hashes.
    D_ASSERT(precomputed_hashes != nullptr);
    precomputed_hashes.CheckValid();

    size_t sel_out_count = count;

    for (idx_t i = 0; i < num_hash_functions; i++) {
        sel_out_count = ProbeInternal(i, *precomputed_hashes, current_sel, sel_out_count, sel);
        std::cout << "Probing " << count << " rows against the bloom-filter; round " << i << " of " << num_hash_functions << " removed " << (count - sel_out_count) << " rows" << std::endl;
        if (sel_out_count == 0) {
            std::cout << "Pruned all rows with bloom-filter" << std::endl;
            return 0;
        }
    }
    std::cout << "Pruned " << (count - sel_out_count) << " rows with Bloom-filter" << std::endl;
    return sel_out_count;
}

} // namespace duckdb
