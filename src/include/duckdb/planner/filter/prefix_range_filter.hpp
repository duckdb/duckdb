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
#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

class PrefixRangeFilter {
public:
	struct BuildState {
		virtual ~BuildState() = default;
	};

	virtual ~PrefixRangeFilter() = default;
	virtual void Initialize(ClientContext &context, idx_t number_of_rows, Value min, Value max) = 0;
	virtual unique_ptr<BuildState> InitializeBuildState(ClientContext &context) const = 0;
	virtual void InsertKeys(Vector &keys, idx_t count, BuildState &state) const = 0;
	virtual void MergeBuildState(BuildState &state) = 0;
	virtual idx_t LookupKeys(Vector &keys, SelectionVector &result_sel, idx_t count) const = 0;
	virtual bool LookupOneValue(const Value &key) const = 0;
	virtual FilterPropagateResult LookupRange(const Value &lower_bound, const Value &upper_bound) const = 0;
	virtual bool IsInitialized() const = 0;

	//! Get the minimum key value (type-erased)
	virtual Value GetMinValue() const = 0;
	//! Get the span (max - min) as uhugeint
	virtual uhugeint_t GetSpan() const = 0;
	//! Get the shift factor (number of bits keys are right-shifted for bucketing)
	virtual idx_t GetShift() const = 0;
	//! Get the number of 64-bit words in the bitmap
	virtual idx_t GetWordCount() const = 0;
	//! Get a read-only pointer to the bitmap data
	virtual const uint64_t *GetBitmapData() const = 0;

	static unique_ptr<PrefixRangeFilter> CreatePrefixRangeFilter(const LogicalType &key_type);
	static bool TryComputeSpan(const Value &lower_bound, const Value &upper_bound, uhugeint_t &result);
};

class PrefixRangeTableFilter final : public TableFilter {
private:
	optional_ptr<PrefixRangeFilter> filter;

	string key_column_name;
	LogicalType key_type;

public:
	static constexpr auto TYPE = TableFilterType::PREFIX_RANGE_FILTER;
	static bool SupportedType(const LogicalType &type);
	static unique_ptr<PrefixRangeFilter> CreateBitmap(const LogicalType &type);

public:
	explicit PrefixRangeTableFilter(optional_ptr<PrefixRangeFilter> filter_p, const string &key_column_name_p,
	                                const LogicalType &key_type_p);

	LogicalType GetKeyType() const {
		return key_type;
	}

	const string &GetKeyColumnName() const {
		return key_column_name;
	}

	const PrefixRangeFilter &GetPrefixRangeFilter() const {
		D_ASSERT(filter);
		return *filter;
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
