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

class BloomFilter {

public:
	BloomFilter() = default;
	void Initialize(ClientContext &context_p, idx_t number_of_rows);

	void InsertHashes(const Vector &hashes_v, idx_t count) const;
	idx_t LookupHashes(const Vector &hashes_v, SelectionVector &result_sel, idx_t count) const;

	void InsertOne(hash_t hash) const;
	bool LookupOne(hash_t hash) const;

	bool IsInitialized() const {
		return initialized;
	}

private:
	idx_t num_sectors;
	uint64_t bitmask; // num_sectors - 1 -> used to get the sector offset

	bool initialized = false;
	AllocatedData buf_;
	uint64_t *bf;
};

class BFTableFilter final : public TableFilter {

private:
	BloomFilter &filter;

	bool filters_null_values;
	string key_column_name;
	LogicalType key_type;

public:
	static constexpr auto TYPE = TableFilterType::BLOOM_FILTER;

public:
	explicit BFTableFilter(BloomFilter &filter_p, const bool filters_null_values_p, const string &key_column_name_p,
	                       const LogicalType &key_type_p)
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

	string ToString(const string &column_name) const override;

	// Filters by first hashing and then probing the bloom filter. The &sel will hold
	// the remaining tuples, &approved_tuple_count will hold the approved count.
	idx_t Filter(Vector &keys_v, SelectionVector &sel, idx_t &approved_tuple_count, BFTableFilterState &state) const;
	bool FilterValue(const Value &value) const;

	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override;

private:
	static void HashInternal(Vector &keys_v, const SelectionVector &sel, const idx_t approved_count,
	                         BFTableFilterState &state);

	bool Equals(const TableFilter &other) const override;
	unique_ptr<TableFilter> Copy() const override;
	unique_ptr<Expression> ToExpression(const Expression &column) const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
