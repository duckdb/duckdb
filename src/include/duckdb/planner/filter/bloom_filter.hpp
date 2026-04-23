//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/bloom_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

//! Runtime bloom-filter state used by join pushdown and internal tablefilter functions.
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

//! DEPRECATED - only preserved for backwards-compatible expression conversion
class LegacyBFTableFilter final : public TableFilter {
private:
	optional_ptr<BloomFilter> filter;

	bool filters_null_values;
	string key_column_name;
	LogicalType key_type;

public:
	static constexpr auto TYPE = TableFilterType::LEGACY_BLOOM_FILTER;

public:
	explicit LegacyBFTableFilter(optional_ptr<BloomFilter> filter_p, const bool filters_null_values_p,
	                             const string &key_column_name_p, const LogicalType &key_type_p)
	    : TableFilter(TYPE), filter(filter_p), filters_null_values(filters_null_values_p),
	      key_column_name(key_column_name_p), key_type(key_type_p) {
	}

private:
	unique_ptr<Expression> ToExpression(const Expression &column) const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
