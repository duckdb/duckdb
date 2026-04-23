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

//! Runtime prefix-range filter state used by join pushdown and internal tablefilter functions.
class PrefixRangeFilter {
public:
	struct BuildState {
		virtual ~BuildState() = default;
		template <class TARGET>

		TARGET &Cast() {
			DynamicCastCheck<TARGET>(this);
			return reinterpret_cast<TARGET &>(*this);
		}
		template <class TARGET>
		const TARGET &Cast() const {
			DynamicCastCheck<TARGET>(this);
			return reinterpret_cast<const TARGET &>(*this);
		}
	};

	virtual ~PrefixRangeFilter() = default;
	virtual void Initialize(ClientContext &context, idx_t number_of_rows, Value min, Value max) = 0;
	virtual unique_ptr<BuildState> InitializeBuildState(ClientContext &context) const = 0;
	virtual void InsertKeys(Vector &keys, idx_t count, BuildState &state) const = 0;
	virtual void MergeBuildState(BuildState &state) = 0;
	virtual idx_t LookupKeys(Vector &keys, SelectionVector &result_sel, idx_t count) const = 0;
	virtual FilterPropagateResult LookupRange(const Value &lower_bound, const Value &upper_bound) const = 0;
	virtual bool IsInitialized() const = 0;
	static bool SupportedType(const LogicalType &type);
	static unique_ptr<PrefixRangeFilter> CreatePrefixRangeFilter(const LogicalType &key_type);
	static bool TryComputeSpan(const Value &lower_bound, const Value &upper_bound, uhugeint_t &result);
};

//! DEPRECATED - only preserved for backwards-compatible expression conversion
class LegacyPrefixRangeTableFilter final : public TableFilter {
private:
	optional_ptr<PrefixRangeFilter> filter;

	string key_column_name;
	LogicalType key_type;

public:
	static constexpr auto TYPE = TableFilterType::LEGACY_PREFIX_RANGE_FILTER;

public:
	explicit LegacyPrefixRangeTableFilter(optional_ptr<PrefixRangeFilter> filter_p, const string &key_column_name_p,
	                                      const LogicalType &key_type_p);

private:
	unique_ptr<Expression> ToExpression(const Expression &column) const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
