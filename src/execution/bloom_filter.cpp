#include "duckdb/execution/bloom_filter.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include <iostream>

namespace duckdb {

BloomFilter::BloomFilter(size_t expected_cardinality, double desired_false_positive_rate, const ClientConfig &config) : num_inserted_keys(0), num_probed_keys(0), num_filtered_keys(0), probing_started(false), config(config) {
    // Approximate size of the Bloom-filter rounded up to the next 8 byte.
    size_t approx_size = static_cast<size_t>(std::ceil(-double(expected_cardinality) * log(desired_false_positive_rate) / 0.48045));
	bloom_filter_size = approx_size + (64 - approx_size % 64);
    num_hash_functions = static_cast<size_t>(std::ceil(approx_size / expected_cardinality * 0.693147));

    bloom_data_buffer.resize(bloom_filter_size / 64, 0);
    bloom_filter.Initialize(bloom_data_buffer.data(), bloom_filter_size);
}

BloomFilter::~BloomFilter() {
}

inline size_t HashToIndex(hash_t hash, size_t bloom_filter_size, size_t i) {
    return (hash >> (i * 1)) % bloom_filter_size;  // TODO: rotation would be a bit nicer because it allows us to generate more values. But C++20 in stdlib.
}

inline void BloomFilter::SetBloomBitsForHashes(size_t fni, Vector &hashes, const SelectionVector &rsel, idx_t count) {
    if(!config.hash_join_bloom_filter) {
        return;
    }

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
    }
    num_inserted_keys += count;
}

inline size_t BloomFilter::ProbeInternal(size_t fni, Vector &hashes, SelectionVector &current_sel, idx_t current_sel_count) {
    D_ASSERT(hashes.GetType().id() == LogicalType::HASH);
    D_ASSERT(current_sel_count > 0); // Should be handled before

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
                auto key_idx = current_sel.get_index(i);
			    auto hash_idx = u_hashes.sel->get_index(key_idx);
                if (u_hashes.validity.RowIsValid(hash_idx)) {
                    auto* hashes = UnifiedVectorFormat::GetData<hash_t>(u_hashes);
                    auto hash = hashes[hash_idx];
                    auto bloom_idx = HashToIndex(hash, bloom_filter_size, fni);
                    if (bloom_filter.RowIsValid(bloom_idx)) {
                        // Bit is set in Bloom-filter. We keep the entry for now.
                        current_sel.set_index(sel_out_idx++, key_idx);
                    }
                }
            }
            return sel_out_idx;
        } else {
            size_t sel_out_idx = 0;
            for (idx_t i = 0; i < current_sel_count; i++) {
                auto key_idx = current_sel.get_index(i);
			    auto hash_idx = u_hashes.sel->get_index(key_idx);
                auto* hashes = UnifiedVectorFormat::GetData<hash_t>(u_hashes);
                auto hash = hashes[hash_idx];
                auto bloom_idx = HashToIndex(hash, bloom_filter_size, fni);
                if (bloom_filter.RowIsValid(bloom_idx)) {
                    // Bit is set in Bloom-filter. We keep the entry for now.
                    current_sel.set_index(sel_out_idx++, key_idx);
                }
            }
            return sel_out_idx;
        }
    }
}


size_t BloomFilter::ProbeWithPrecomputedHashes(const SelectionVector *&current_sel, idx_t count, SelectionVector &sel, Vector &precomputed_hashes) {
    if(!config.hash_join_bloom_filter) {
        return count;
    }
    
    probing_started = true;
    num_probed_keys += count;

    // Copy current selection vector over to temporary selection vector
    SelectionVector sel_tmp;
    size_t sel_tmp_count = count;
    for (idx_t i = 0; i < count; i++) {
        // TODO: ideally, we should only do this if current_sel.IsSet()
        sel_tmp.set_index(i, current_sel->get_index(i));
    }

    // Perform probing
    for (idx_t i = 0; i < num_hash_functions; i++) {
        sel_tmp_count = ProbeInternal(i, precomputed_hashes, sel_tmp, sel_tmp_count);
        if (sel_tmp_count == 0) {
            num_filtered_keys += count;
            return 0;
        }
    }

    // Copy result of temporary selection vector over to helper selection vector and adjust pointers.
    current_sel = FlatVector::IncrementalSelectionVector();
    for (idx_t i = 0; i < sel_tmp_count; i++) {
        sel.set_index(i, sel_tmp.get_index(i));
    }
    // Swap out the given selection vector with the modified one.
    current_sel = &sel;

    num_filtered_keys += (count - sel_tmp_count);
    return sel_tmp_count;
}

} // namespace duckdb
