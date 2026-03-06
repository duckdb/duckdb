//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/prefix_range_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

class PrefixRangeFilter {
public:
	virtual ~PrefixRangeFilter() = default;
	virtual void Initialize(ClientContext &context, idx_t number_of_rows, Value min, Value max) = 0;
	virtual void InsertKeys(Vector &keys, idx_t count) const = 0;
	virtual void InsertOne(const Value &key) const = 0;
	virtual idx_t LookupKeys(Vector &keys, SelectionVector &result_sel, idx_t count) const = 0;
	virtual bool LookupOneValue(const Value &key) const = 0;
	virtual FilterPropagateResult LookupRange(const Value &lower_bound, const Value &upper_bound) const = 0;
	virtual bool IsInitialized() const = 0;
	static unique_ptr<PrefixRangeFilter> CreatePrefixRangeFilter(const LogicalType &key_type);
};

class PrefixRangeTableFilter final : public TableFilter {
private:
	optional_ptr<PrefixRangeFilter> filter;

	bool filters_null_values;
	string key_column_name;
	LogicalType key_type;

public:
	static constexpr auto TYPE = TableFilterType::PREFIX_RANGE_FILTER;
	static bool SupportedType(const LogicalType &type);
	static unique_ptr<PrefixRangeFilter> CreateBitmap(const LogicalType &type);

public:
	explicit PrefixRangeTableFilter(optional_ptr<PrefixRangeFilter> filter_p, const bool filters_null_values_p,
	                                const string &key_column_name_p, const LogicalType &key_type_p);

	//! If the join condition is e.g. "A = B", the bf will filter null values.
	//! If the condition is "A is B" the filter will let nulls pass
	bool FiltersNullValues() const {
		return filters_null_values;
	}

	LogicalType GetKeyType() const {
		return key_type;
	}

	string ToString(const string &column_name) const override;

	idx_t Filter(Vector &keys, SelectionVector &sel, idx_t &approved_tuple_count) const;
	bool FilterValue(const Value &value) const;

	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override;

private:
	bool Equals(const TableFilter &other) const override;
	unique_ptr<TableFilter> Copy() const override;
	unique_ptr<Expression> ToExpression(const Expression &column) const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
