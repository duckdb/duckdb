
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/optional_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/table_filter_state.hpp"

namespace duckdb {

class OptionalFilter : public TableFilter {
public:
	static constexpr auto TYPE = TableFilterType::OPTIONAL_FILTER;

public:
	explicit OptionalFilter(unique_ptr<TableFilter> filter = nullptr);

	//! Optional child filters.
	unique_ptr<TableFilter> child_filter;

public:
	string ToString(const string &column_name) const override;
	unique_ptr<TableFilter> Copy() const override;
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);

	//! If the child filter should be executed, the optional_ptr will contain the SelectivityOptionalFilterState
	virtual optional_ptr<SelectivityOptionalFilterState> ExecuteChildFilter(TableFilterState &filter_state) const {
		return nullptr;
	}

	virtual unique_ptr<TableFilterState> InitializeState(ClientContext &context) const {
		// root nodes - create an empty filter state
		return make_uniq<TableFilterState>();
	}
};

} // namespace duckdb
