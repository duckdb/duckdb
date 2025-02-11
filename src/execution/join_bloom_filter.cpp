#include "duckdb/execution/join_bloom_filter.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include <iostream>

namespace duckdb {

// Maximum number of hash functions used in the bloom-filter.
static size_t MAX_HASH_FUNCTIONS = 2;
// Maximum size of the bloom-filter in bits. Set to 0 for unbounded.
static size_t MAX_BF_SIZE_BITS = 0;
// Whether to use a fixed-size bloom-filter.
static bool USE_BABY_BLOOM = false;
// Whether the bloom-filter should have a power-of-two size. If yes, indexing can be done fast with a bitmap. Otherwise, fast-mod is used.
static bool USE_POWER_2_BF_SIZE = true;
// The minimum number of keys that need to be probed before the threshold for disabling probing is applied.
static size_t PROBE_MIN_KEYS_BEFORE_THRESHOLD = 4000;
// The minimum acceptable selectivity. If the selectivity is lower, probing will be turned off.
static double PROBE_SELECTIVITY_THRESHOLD = 0.6;


size_t ComputeBloomFilterSize(size_t expected_cardinality, double desired_false_positive_rate) {
    if (USE_BABY_BLOOM) {
        return 8 * 1024 * 8;
    }

    // The theoretically optimal size of the Bloom-filter.
    size_t approx_size = static_cast<size_t>(std::ceil(-double(expected_cardinality) * log(desired_false_positive_rate) / 0.48045));

    if (USE_POWER_2_BF_SIZE) {
        // Approximate size rounded to the next power of 2
        size_t v = approx_size;
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;

        if (MAX_BF_SIZE_BITS > 0) {
            return MinValue(v, MAX_BF_SIZE_BITS);
        } else {
            return v;
        }
    } else {
        // Approximate size of the Bloom-filter rounded up to the next 8 bytes.
	    size_t v = approx_size + (64 - approx_size % 64);

        if (MAX_BF_SIZE_BITS > 0) {
            return MinValue(v, MAX_BF_SIZE_BITS);
        } else {
            return v;
        }
    }
}

size_t ComputeNumHashFunctions(size_t expected_cardinality, size_t bloom_filter_size) {
    if (USE_BABY_BLOOM) {
        return 4;
    }

    // The theoretically optimal number of hash functions.
    const size_t theoretical_optimum = static_cast<size_t>(std::ceil(bloom_filter_size / expected_cardinality * 0.693147));
    // Limit the number of hash functions because too many harm performance.
    return MinValue(MAX_HASH_FUNCTIONS, theoretical_optimum);
}

inline size_t JoinBloomFilter::HashToIndex(hash_t hash, size_t i) const {
    // Rotate a single hash to produce multiple hashes. Shift by 32 digits to produce up to 2 non-overlapping hash function.
    const auto rot_hash = hash >> ((i << 5));
    
    if (USE_BABY_BLOOM) {
        return rot_hash & 0xffff;
    } else if (USE_POWER_2_BF_SIZE) {
        return rot_hash & bitmask;
    } else {
        // Fast modulo reduction: https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
        return ((rot_hash & 0xffffffff) * (bloom_filter_size & 0xffffffff)) >> 32;
    }
}

JoinBloomFilter::JoinBloomFilter(size_t expected_cardinality, double desired_false_positive_rate, vector<column_t> column_ids) 
: column_ids(std::move(column_ids)) {
	bloom_filter_size = ComputeBloomFilterSize(expected_cardinality, desired_false_positive_rate);
    num_hash_functions = ComputeNumHashFunctions(expected_cardinality, bloom_filter_size);

    bloom_data_buffer.resize(bloom_filter_size / 64, 0);
    bloom_filter_bits.Initialize(bloom_data_buffer.data(), bloom_filter_size);

    if (USE_POWER_2_BF_SIZE) {
        size_t v = bloom_filter_size;
        bitmask = 0;
        while (v > 1) {
            v = v >> 1;
            bitmask = bitmask << 1 | 0x1;
        }
    }
}

JoinBloomFilter::JoinBloomFilter(vector<column_t> column_ids, size_t num_hash_functions, size_t bloom_filter_size) : num_hash_functions(num_hash_functions), bloom_filter_size(bloom_filter_size), column_ids(std::move(column_ids))  {
    bloom_data_buffer.resize(bloom_filter_size / 64, 0);
    bloom_filter_bits.Initialize(bloom_data_buffer.data(), bloom_filter_size);
}

JoinBloomFilter::~JoinBloomFilter() {
}

inline void JoinBloomFilter::SetBloomBitsForHashes(size_t fni, Vector &hashes, const SelectionVector &sel, idx_t count) {
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
                auto key_idx = sel.get_index(i);
			    auto hash_idx = u_hashes.sel->get_index(key_idx);
                if (u_hashes.validity.RowIsValid(hash_idx)) {
                    auto hash = UnifiedVectorFormat::GetData<hash_t>(u_hashes)[hash_idx];
                    auto bloom_idx = HashToIndex(hash, fni);
                    bloom_filter_bits.SetValid(bloom_idx);
                }
            }
        } else {
            for (idx_t i = 0; i < count; i++) {
                auto key_idx = sel.get_index(i);
			    auto hash_idx = u_hashes.sel->get_index(key_idx);
                auto* hashes = UnifiedVectorFormat::GetData<hash_t>(u_hashes);
                auto hash = hashes[hash_idx];
                auto bloom_idx = HashToIndex(hash, fni);
                bloom_filter_bits.SetValid(bloom_idx);
            }
        }
    }
}

void JoinBloomFilter::BuildWithPrecomputedHashes(Vector &hashes, const SelectionVector &sel, idx_t count) {
    Profiler p;
    p.Start();

    // Rotate hash by a couple of bits to produce a new "hash value".
    // With this trick, keys have to be hashed only once.
    for (idx_t i = 0; i < num_hash_functions; i++) {
        SetBloomBitsForHashes(i, hashes, sel, count);
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
		auto bloom_idx = HashToIndex(*hash, fni);

        if (bloom_filter_bits.RowIsValid(bloom_idx)) {
            // All constant elements match. No need to modify the selection vector.
            return current_sel_count;
        } else {
            return 0;
        }
    } else {
        UnifiedVectorFormat u_hashes;
		hashes.ToUnifiedFormat(current_sel_count, u_hashes);

        if (!u_hashes.validity.AllValid()) {
            size_t sel_out_idx = 0;
            SelectionVector tmp_sel(current_sel_count);
            for (idx_t i = 0; i < current_sel_count; i++) {
                auto key_idx = current_sel.get_index(i);
			    auto hash_idx = u_hashes.sel->get_index(key_idx);
                if (u_hashes.validity.RowIsValid(hash_idx)) {
                    auto* hashes = UnifiedVectorFormat::GetData<hash_t>(u_hashes);
                    auto hash = hashes[hash_idx];
                    auto bloom_idx = HashToIndex(hash, fni);
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
                auto bloom_idx = HashToIndex(hash, fni);
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


size_t JoinBloomFilter::ProbeWithPrecomputedHashes(Vector &precomputed_hashes, SelectionVector &sel, idx_t count) {
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

bool JoinBloomFilter::ShouldDiscardAfterBuild() const {
    if (USE_BABY_BLOOM) {
        // only use bloom filter for probing if it's false-positive-rate is low enough (<=1.3%)
        return GetScarcity() > 0.34;
    }

    return GetScarcity() > 0.34;
}

bool JoinBloomFilter::ShouldStopProbing() const {
    return num_probed_keys > PROBE_MIN_KEYS_BEFORE_THRESHOLD && GetObservedSelectivity() < PROBE_SELECTIVITY_THRESHOLD;
}

void JoinBloomFilter::PrintBuildStats() const {
    std::cout << "    \"bf_num_hash_functions\": " << num_hash_functions << "," << std::endl;
    std::cout << "    \"bf_size_bits\": " << bloom_filter_size << "," << std::endl;
    std::cout << "    \"bf_scarcity\": " << GetScarcity() << "," << std::endl;
    std::cout << "    \"bf_inserted_keys\": " << num_inserted_keys << "," << std::endl;
    std::cout << "    \"bf_bitmask\": " << bitmask << "," << std::endl;
    std::cout << "    \"build_time\": " << build_time << "," << std::endl;
}

void JoinBloomFilter::PrintProbeStats() const {
    std::cout << "    \"selectivity\": " << GetObservedSelectivity() << "," << std::endl;
    std::cout << "    \"probed_keys\": " << GetNumProbedKeys() << "," << std::endl;
    std::cout << "    \"probe_time\": " << probe_time << "," << std::endl;
    std::cout << "    \"hash_time\": " << hash_time << "," << std::endl;
}

} // namespace duckdb
