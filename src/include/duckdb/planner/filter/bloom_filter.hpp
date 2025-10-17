//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/bloom_filter
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

class CacheSectorizedBloomFilter {

	static constexpr idx_t MAX_NUM_SECTORS = (1ULL << 26);
	static constexpr idx_t MIN_NUM_BITS_PER_KEY = 16;
	static constexpr idx_t MIN_NUM_BITS = 512;
	static constexpr idx_t LOG_SECTOR_SIZE = 5;
	static constexpr idx_t SIMD_BATCH_SIZE = 16;

public:
	CacheSectorizedBloomFilter() = default;
	void Initialize(ClientContext &context_p, idx_t est_num_rows);

	idx_t LookupHashes(const Vector &hashes, SelectionVector &result_sel, idx_t count) const;
	bool LookupHash(hash_t hash) const;
	void InsertHashes(const Vector &hashes, idx_t count) const;

	bool IsInitialized() const {
		return initialized;
	}

	bool IsActive() const {
		return active;
	}

	void SetActive(const bool val) {
		active = val;
	}

private:
	bool initialized = false;
	bool active = false;

	idx_t num_sectors;
	idx_t num_sectors_log;

	ClientContext *context;
	BufferManager *buffer_manager;
	uint32_t *blocks;

	// key_lo |5:bit3|5:bit2|5:bit1|  13:block    |4:sector1 | bit layout (32:total)
	// key_hi |5:bit4|5:bit3|5:bit2|5:bit1|9:block|3:sector2 | bit layout (32:total)
	static uint32_t GetMask1(uint32_t key_lo);
	static uint32_t GetMask2(uint32_t key_hi);

	uint32_t GetSector1(uint32_t key_lo, uint32_t key_hi) const;
	uint32_t GetSector2(uint32_t key_hi, uint32_t block1) const;

	void InsertOne(uint32_t key_lo, uint32_t key_hi, uint32_t *bf) const;
	bool LookupOne(uint32_t key_lo, uint32_t key_hi, const uint32_t *__restrict bf) const;

private:
	idx_t BloomFilterLookup(const hash_t *__restrict hashes, const uint32_t *__restrict bf, SelectionVector &found_sel,
	                        const idx_t num) const {
		const auto key = reinterpret_cast<const uint32_t *__restrict>(hashes);
		idx_t found_count = 0;
		for (idx_t i = 0; i + SIMD_BATCH_SIZE <= num; i += SIMD_BATCH_SIZE) {
			uint32_t block1[SIMD_BATCH_SIZE], mask1[SIMD_BATCH_SIZE];
			uint32_t block2[SIMD_BATCH_SIZE], mask2[SIMD_BATCH_SIZE];

			for (idx_t j = 0; j < SIMD_BATCH_SIZE; j++) {
				idx_t p = i + j;
				const uint32_t key_lo = key[p + p];
				const uint32_t key_hi = key[p + p + 1];
				block1[j] = GetSector1(key_lo, key_hi);
				mask1[j] = GetMask1(key_lo);
				block2[j] = GetSector2(key_hi, block1[j]);
				mask2[j] = GetMask2(key_hi);
			}

			for (idx_t j = 0; j < SIMD_BATCH_SIZE; j++) {
				const bool hit = ((bf[block1[j]] & mask1[j]) == mask1[j]) & ((bf[block2[j]] & mask2[j]) == mask2[j]);
				found_sel.set_index(found_count, i + j);
				found_count += hit;
			}
		}

		// unaligned tail
		for (idx_t i = num & ~(SIMD_BATCH_SIZE - 1); i < num; i++) {
			const bool hit = LookupOne(key[i + i], key[i + i + 1], bf);
			found_sel.set_index(found_count, i);
			found_count += hit;
		}
		return found_count;
	}

	void BloomFilterInsert(const idx_t num, const uint64_t *__restrict key64, uint32_t *__restrict bf) const {
		const uint32_t *__restrict key = reinterpret_cast<const uint32_t *__restrict>(key64);
		for (idx_t i = 0; i + SIMD_BATCH_SIZE <= num; i += SIMD_BATCH_SIZE) {
			uint32_t block1[SIMD_BATCH_SIZE], mask1[SIMD_BATCH_SIZE];
			uint32_t block2[SIMD_BATCH_SIZE], mask2[SIMD_BATCH_SIZE];

			for (idx_t j = 0; j < SIMD_BATCH_SIZE; j++) {
				idx_t p = i + j;
				uint32_t key_lo = key[p + p];
				uint32_t key_hi = key[p + p + 1];
				block1[j] = GetSector1(key_lo, key_hi);
				mask1[j] = GetMask1(key_lo);
				block2[j] = GetSector2(key_hi, block1[j]);
				mask2[j] = GetMask2(key_hi);
			}

			for (idx_t j = 0; j < SIMD_BATCH_SIZE; j++) {
				// Atomic OR operation
				std::atomic<uint32_t> &atomic_bf1 = *reinterpret_cast<std::atomic<uint32_t> *>(&bf[block1[j]]);
				std::atomic<uint32_t> &atomic_bf2 = *reinterpret_cast<std::atomic<uint32_t> *>(&bf[block2[j]]);

				atomic_bf1.fetch_or(mask1[j], std::memory_order_relaxed);
				atomic_bf2.fetch_or(mask2[j], std::memory_order_relaxed);
			}
		}

		// unaligned tail
		for (idx_t i = num & ~(SIMD_BATCH_SIZE - 1); i < num; i++) {
			InsertOne(key[i + i], key[i + i + 1], bf);
		}
	}

	AllocatedData buf_;
};

class BloomFilter : public TableFilter {

private:
	CacheSectorizedBloomFilter &filter;

	bool filters_null_values;
	string key_column_name;
	LogicalType key_type;

public:
	static constexpr auto TYPE = TableFilterType::BLOOM_FILTER;

public:
	explicit BloomFilter(CacheSectorizedBloomFilter &filter_p, const bool filters_null_values_p,
	                     const string &key_column_name_p, const LogicalType &key_type_p)
	    : TableFilter(TYPE), filter(filter_p), filters_null_values(filters_null_values_p),
	      key_column_name(key_column_name_p), key_type(key_type_p) {
	}

	//! If the join condition is e.g. "A = B", the bf will filter null values.
	//! If the condition is "A is B" the filter will let nulls pass
	bool FiltersNullValues() const {
		return filters_null_values;
	}

	LogicalType GetKeyType() const {
		return key_type;
	}

public:
	string ToString(const string &column_name) const override;

	static void HashInternal(Vector &keys_v, const SelectionVector &sel, idx_t &approved_count,
	                         BloomFilterState &state) {
		if (sel.IsSet()) {
			state.keys_sliced_v.Slice(keys_v, sel, approved_count);
			VectorOperations::Hash(state.keys_sliced_v, state.hashes_v, approved_count);
		} else {
			VectorOperations::Hash(keys_v, state.hashes_v, approved_count);
		}
	}

	bool FilterInitializedAndActive() const {
		return this->filter.IsInitialized() && this->filter.IsActive();
	}

	// Filters the data by first hashing and then probing the bloom filter. The &sel will hold
	// the remaining tuples, &approved_tuple_count will hold the approved count.
	__attribute__((noinline)) idx_t Filter(Vector &keys_v, UnifiedVectorFormat &keys_uvf, SelectionVector &sel,
	                                       idx_t &approved_tuple_count, BloomFilterState &state) const {

		if (!FilterInitializedAndActive()) {
			return approved_tuple_count;
		}

		if (state.current_capacity < approved_tuple_count) {
			state.hashes_v.Initialize(approved_tuple_count);
			state.bf_sel.Initialize(approved_tuple_count);
			state.current_capacity = approved_tuple_count;
		}

		HashInternal(keys_v, sel, approved_tuple_count, state);

		idx_t found_count;
		if (state.hashes_v.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			const auto constant_hash = *ConstantVector::GetData<hash_t>(state.hashes_v);
			const bool found = this->filter.LookupHash(constant_hash);
			found_count = found ? approved_tuple_count : 0;
		} else {
			state.hashes_v.Flatten(approved_tuple_count);
			found_count = this->filter.LookupHashes(state.hashes_v, state.bf_sel, approved_tuple_count);
		}

		// add the runtime statistics to stop using the bf if not selective
		if (state.vectors_processed < 20) {
			state.vectors_processed += 1;
			state.tuples_accepted += found_count;
			state.tuples_processed += approved_tuple_count;

			if (state.vectors_processed == 20) {
				const double selectivity =
				    static_cast<double>(state.tuples_accepted) / static_cast<double>(state.tuples_processed);
				if (selectivity > 0.5) {
					this->filter.SetActive(false);
				}
			}
		}

		// all the elements have been found, we don't need to translate anything
		if (found_count == approved_tuple_count) {
			return approved_tuple_count;
		}

		if (sel.IsSet()) {
			for (idx_t idx = 0; idx < found_count; idx++) {
				const idx_t flat_sel_idx = state.bf_sel.get_index(idx);
				const idx_t original_sel_idx = sel.get_index(flat_sel_idx);
				sel.set_index(idx, original_sel_idx);
			}
		} else {
			sel.Initialize(state.bf_sel);
		}

		approved_tuple_count = found_count;
		return approved_tuple_count;
	}

	bool FilterValue(const Value &value) const {
		const auto hash = value.Hash();
		return filter.LookupHash(hash);
	}

	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override {
		if (FilterInitializedAndActive()) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		return FilterPropagateResult::FILTER_ALWAYS_TRUE;
	}

	bool Equals(const TableFilter &other) const override {
		if (!TableFilter::Equals(other)) {
			return false;
		}
		// todo: How to compare bloom filters?
		return false;
	}
	unique_ptr<TableFilter> Copy() const override {
		return make_uniq<BloomFilter>(this->filter, this->filters_null_values, this->key_column_name, this->key_type);
	}

	unique_ptr<Expression> ToExpression(const Expression &column) const override;

	void Serialize(Serializer &serializer) const override {
		TableFilter::Serialize(serializer);
		serializer.WriteProperty<bool>(200, "filters_null_values", filters_null_values);
		serializer.WriteProperty<string>(201, "key_column_name", key_column_name);
		serializer.WriteProperty<LogicalType>(202, "key_type", key_type);
		// todo: How/Should be serialize the bloom filter?
	}
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer) {
		auto filters_null_values = deserializer.ReadProperty<bool>(200, "filters_null_values");
		auto key_column_name = deserializer.ReadProperty<string>(201, "key_column_name");
		auto key_type = deserializer.ReadProperty<LogicalType>(202, "key_type");

		CacheSectorizedBloomFilter filter;
		auto result = make_uniq<BloomFilter>(filter, filters_null_values, key_column_name, key_type);
		return std::move(result);
	}
};

} // namespace duckdb
