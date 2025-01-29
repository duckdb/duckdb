#include "duckdb/execution/join_bloom_filter.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include <iostream>

namespace duckdb {

JoinBloomFilter::JoinBloomFilter(size_t expected_cardinality, double desired_false_positive_rate, vector<column_t> column_ids, uint64_t bitmask) 
: column_ids(std::move(column_ids)), bitmask(bitmask) {
    int b_ones = 0;
    while ((bitmask >> b_ones) > 0) {
        b_ones++;
    }

    // Approximate size of the Bloom-filter rounded up to the next 8 byte.
    size_t approx_size = static_cast<size_t>(std::ceil(-double(expected_cardinality) * log(desired_false_positive_rate) / 0.48045));
	bloom_filter_size = approx_size + (64 - approx_size % 64);
    num_hash_functions = MinValue(4, static_cast<int>(std::ceil(approx_size / expected_cardinality * 0.693147)));  // Limit to up to 3 hash functions for performance.

    bloom_data_buffer.resize(bloom_filter_size / 64, 0);
    bloom_filter_bits.Initialize(bloom_data_buffer.data(), bloom_filter_size);
}

JoinBloomFilter::JoinBloomFilter(vector<column_t> column_ids, size_t num_hash_functions, size_t bloom_filter_size) : num_hash_functions(num_hash_functions), bloom_filter_size(bloom_filter_size), column_ids(std::move(column_ids))  {
    bloom_data_buffer.resize(bloom_filter_size / 64, 0);
    bloom_filter_bits.Initialize(bloom_data_buffer.data(), bloom_filter_size);
}

JoinBloomFilter::~JoinBloomFilter() {
}

inline size_t JoinBloomFilter::HashToIndex(hash_t hash, size_t i) const {
    // Create different hashes out of a single hash by shifting a variable length of bits.
    //std::cout << "inserting hash " << hash << std::endl;
    //const auto h = hash & bitmask;
    const auto r = (hash >> ((i << 4))) & 0xffffffff;
    // Fast modulo reduction: https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
    return r % bloom_filter_size;
    //return (r * (bloom_filter_size & 0xffffffff)) >> 32;
}

inline void JoinBloomFilter::SetBloomBitsForHashes(size_t fni, Vector &hashes, const SelectionVector &rsel, idx_t count) {
    D_ASSERT(hashes.GetType().id() == LogicalType::HASH);
    D_ASSERT(probing_started == false);

    if (hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
        auto hash = ConstantVector::GetData<hash_t>(hashes);
		auto bloom_idx = HashToIndex(*hash, fni);
        bloom_filter_bits.SetValid(bloom_idx);
    } else {
        UnifiedVectorFormat u_hashes;
		hashes.ToUnifiedFormat(count, u_hashes);

        if (!u_hashes.validity.AllValid()) {
            for (idx_t i = 0; i < count; i++) {
                auto key_idx = rsel.get_index(i);
			    auto hash_idx = u_hashes.sel->get_index(key_idx);
                if (u_hashes.validity.RowIsValid(hash_idx)) {
                    auto hash = UnifiedVectorFormat::GetData<hash_t>(u_hashes)[hash_idx];
                    auto bloom_idx = HashToIndex(hash, fni);
                    bloom_filter_bits.SetValid(bloom_idx);
                }
            }
        } else {
            for (idx_t i = 0; i < count; i++) {
                auto key_idx = rsel.get_index(i);
			    auto hash_idx = u_hashes.sel->get_index(key_idx);
                auto* hashes = UnifiedVectorFormat::GetData<hash_t>(u_hashes);
                auto hash = hashes[hash_idx];
                //std::cout << "inserting hash " << hash << std::endl;
                auto bloom_idx = HashToIndex(hash, fni);
                //std::cout << "idx="<<bloom_idx << std::endl;
                bloom_filter_bits.SetValid(bloom_idx);
            }
        }
    }
}

void JoinBloomFilter::BuildWithPrecomputedHashes(Vector &hashes, const SelectionVector &rsel, idx_t count) {
    Profiler p;
    p.Start();

    // Rotate hash by a couple of bits to produce a new "hash value".
    // With this trick, keys have to be hashed only once.
    for (idx_t i = 0; i < num_hash_functions; i++) {
        SetBloomBitsForHashes(i, hashes, rsel, count);
    }
    num_inserted_keys += count;

    p.End();
    build_time += p.Elapsed();
}

inline size_t JoinBloomFilter::ProbeInternal(size_t fni, Vector &hashes, SelectionVector &current_sel, idx_t current_sel_count) const {
    D_ASSERT(hashes.GetType().id() == LogicalType::HASH);
    D_ASSERT(current_sel_count > 0); // Should be handled before

    if (hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
        auto hash = ConstantVector::GetData<hash_t>(hashes);
		auto bloom_idx = HashToIndex(ht_entry_t::ExtractSalt(*hash) ^ (*hash & bitmask), fni);

        if (bloom_filter_bits.RowIsValid(bloom_idx)) {
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
            SelectionVector tmp_sel(current_sel_count);
            for (idx_t i = 0; i < current_sel_count; i++) {
                // TODO: We can skip this row if it was already removed by a previous iteration
                auto key_idx = current_sel.get_index(i);
			    auto hash_idx = u_hashes.sel->get_index(key_idx);
                if (u_hashes.validity.RowIsValid(hash_idx)) {
                    auto* hashes = UnifiedVectorFormat::GetData<hash_t>(u_hashes);
                    auto hash = hashes[hash_idx];
                    auto bloom_idx = HashToIndex(ht_entry_t::ExtractSalt(hash) ^ (hash & bitmask), fni);
                    if (bloom_filter_bits.RowIsValid(bloom_idx)) {
                        // Bit is set in Bloom-filter. We keep the entry for now.
                        tmp_sel.set_index(sel_out_idx++, key_idx);
                    }
                }
            }
            current_sel.Initialize(tmp_sel);
            return sel_out_idx;
        } else {
            size_t sel_out_idx = 0;
            SelectionVector tmp_sel(current_sel_count);
            for (idx_t i = 0; i < current_sel_count; i++) {
                auto key_idx = current_sel.get_index(i);
			    auto hash_idx = u_hashes.sel->get_index(key_idx);
                auto* hashes = UnifiedVectorFormat::GetData<hash_t>(u_hashes);
                auto hash = hashes[hash_idx];
                auto bloom_idx = HashToIndex(ht_entry_t::ExtractSalt(hash) ^ (hash & bitmask), fni);
                if (bloom_filter_bits.RowIsValid(bloom_idx)) {
                    // Bit is set in Bloom-filter. We keep the entry for now.
                    tmp_sel.set_index(sel_out_idx++, key_idx);
                }
            }
            current_sel.Initialize(tmp_sel);
            return sel_out_idx;
        }
    }
}


size_t JoinBloomFilter::ProbeWithPrecomputedHashes(SelectionVector &sel, idx_t count, Vector &precomputed_hashes) {
    Profiler p;
    p.Start();

    //probing_started = true;
    num_probed_keys += count;

    size_t sel_tmp_count = count;

    // Perform probing
    for (idx_t i = 0; i < num_hash_functions; i++) {
        sel_tmp_count = ProbeInternal(i, precomputed_hashes, sel, sel_tmp_count);
        if (sel_tmp_count == 0) {
            // All keys have been removed. No need to continue with further rounds.
            break;
        }
    }

    num_filtered_keys += (count - sel_tmp_count);

    p.End();
    probe_time += p.Elapsed();
    return sel_tmp_count;
}

} // namespace duckdb
